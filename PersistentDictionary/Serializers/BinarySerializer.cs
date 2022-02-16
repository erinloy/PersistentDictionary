using FASTER.core;

namespace PersistentDictionary.Serializers
{
  public sealed class BinarySerializer : BinaryObjectSerializer<byte[]>
  {
    public override void Deserialize(out byte[] obj)
    {
      int count = reader.ReadInt32();
      obj = reader.ReadBytes(count);
    }

    public override void Serialize(ref byte[] obj)
    {
      writer.Write(obj.Length);
      writer.Write(obj);
    }
  }
}
