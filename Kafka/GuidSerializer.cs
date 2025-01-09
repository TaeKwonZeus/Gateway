using Confluent.Kafka;

namespace Gateway.Kafka;

public class GuidSerializer : ISerializer<Guid>, IDeserializer<Guid>
{
    public byte[] Serialize(Guid data, SerializationContext context) => data.ToByteArray();

    public Guid Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        isNull ? Guid.Empty : new Guid(data);
}