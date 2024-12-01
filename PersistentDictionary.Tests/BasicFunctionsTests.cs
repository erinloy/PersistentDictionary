using NUnit.Framework;
using PersistentDictionary.Serializers;
using System.IO;
using System.Threading.Tasks;

namespace PersistentDictionary.Tests
{
  public class BasicFunctionsTests
  {
    private const string _folder = "./BasicTestData";

    Dictionary<string, string> _persistentDictionary;

    [SetUp]
    public void Setup()
    {
      _persistentDictionary?.Dispose();

      if(Directory.Exists(_folder)) Directory.Delete(_folder, true);

      _persistentDictionary = new Dictionary<string, string>(_folder, new StringSerializer(), new StringSerializer());
    }

    [Test]
    public async Task ReadWrite()
    {
      var key = "TEST";
      var value = "TEST VALUE";

      await _persistentDictionary.Write(key, value);

      Assert.That(value == await _persistentDictionary.Read(key));
    }

    [Test]
    public async Task Count()
    {
      var key = "TEST";
      var value = "TEST VALUE";
      var count = 100;

      for (int i = 0; i < count; i++)
      {
        await _persistentDictionary.Write($"{key} {i}", value);
      }
      
      Assert.That(count == await _persistentDictionary.Count());
    }

    [Test]
    public async Task Iterate()
    {
      var key = "TEST";
      var value = "TEST VALUE";
      var count = 100;

      for (int i = 0; i < count; i++)
      {
        await _persistentDictionary.Write($"{key} {i}", value);
      }

      var expectedCount = 0;

      await _persistentDictionary.Iterate((key, value) =>
      {
        expectedCount++;
        return Task.CompletedTask;
      });

      Assert.That(count == expectedCount);
    }
  }
}