using BinaryPack;
using FASTER.core;

namespace PersistentDictionary.Serializers
{
  public sealed class ObjectSerializer<T> : BinaryObjectSerializer<T> where T : new()
  {
    public override void Deserialize(out T obj)
    {
      int count = reader.ReadInt32();
      var byteArray = reader.ReadBytes(count);
      obj = BinaryConverter.Deserialize<T>(byteArray);
    }
    
    public override void Serialize(ref T obj)
    {
      var byteArray = BinaryConverter.Serialize(obj);
      writer.Write(byteArray.Length);
      writer.Write(byteArray);
    }
  }
}
