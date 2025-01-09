using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Gateway.Kafka;

using Msg = Message<Guid, byte[]>;

public sealed class KafkaService : BackgroundService
{
    private readonly string[] _consumeTopics = [];

    private readonly ProducerConfig _producerConfig = new()
    {
        BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_SERVER") ?? "localhost:9092",
    };

    private readonly ConsumerConfig _consumerConfig = new()
    {
        BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_SERVER") ?? "localhost:9092",
        GroupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID") ?? "gateway"
    };

    private readonly IProducer<Guid, byte[]> _producer;

    private readonly IConsumer<Guid, byte[]> _consumer;

    private readonly ILogger<KafkaService> _logger;

    public KafkaService(ILogger<KafkaService> logger)
    {
        _logger = logger;

        var serde = new GuidSerializer();

        _producer = new ProducerBuilder<Guid, byte[]>(_producerConfig).SetKeySerializer(serde).Build();
        _consumer = new ConsumerBuilder<Guid, byte[]>(_consumerConfig).SetKeyDeserializer(serde).Build();

        _logger.LogInformation("Initialized successfully.");
    }

    private CancellationToken _cts = CancellationToken.None;

    private readonly ConcurrentDictionary<(string topic, Guid id), Action<Msg>> _callbacks = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _cts = stoppingToken;

        foreach (var topic in _consumeTopics) _consumer.Subscribe(topic);
        _logger.LogInformation("Subscribed to topics.");

        var tcs = new TaskCompletionSource();

        // Consume is blocking so we have to start a separate thread
        var thread = new Thread(() =>
        {
            while (true)
            {
                var cr = _consumer.Consume(_cts);
                try
                {
                    HandleReceived(cr);
                }
                catch (OperationCanceledException e)
                {
                    tcs.SetResult();
                    return;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Failed to handle consumed message from topic {} with key {}",
                        cr.Topic, cr.Message.Key);
                }
            }
        });
        thread.Start();

        // Return when the thread exits
        await tcs.Task;
    }

    private void HandleReceived(ConsumeResult<Guid, byte[]> cr)
    {
        var topic = cr.Topic;
        var key = cr.Message.Key;

        if (_callbacks.TryGetValue((topic, key), out var callback))
            callback(cr.Message);
    }

    public Task Send(string topic, Guid key, byte[] value) =>
        _producer.ProduceAsync(topic, new Msg { Key = key, Value = value }, _cts);

    public Task Send(string topic, byte[] value) =>
        _producer.ProduceAsync(topic, new Msg { Value = value }, _cts);

    /// <summary>
    /// Send a message to the specified topic and wait for a response.
    /// </summary>
    /// <param name="sendTo">The topic to send to.</param>
    /// <param name="receiveFrom">The topic to receive from.</param>
    /// <param name="value">The value to send.</param>
    /// <returns>A tuple of the topic and the message.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown if receiveFrom isn't being listened to or
    /// Send throws an ArgumentException.
    /// </exception>
    public async Task<Msg> SendRpc(string sendTo, string receiveFrom, byte[] value)
    {
        var key = Guid.NewGuid();
        var tcs = new TaskCompletionSource<Msg>();

        _callbacks[(receiveFrom, key)] = msg => tcs.SetResult(msg);

        try
        {
            await Send(sendTo, key, value);
            return await tcs.Task;
        }
        finally
        {
            _callbacks.TryRemove((receiveFrom, key), out _);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        _producer.Dispose();
        _consumer.Dispose();
    }
}
