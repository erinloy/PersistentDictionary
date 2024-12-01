using NUnit.Framework;
using PersistentDictionary.Serializers;
using System.IO;
using System.Threading.Tasks;

namespace PersistentDictionary.Tests
{
  public class RecoveringTests
  {
    private const string _folder = "./RecoveringTestData";

    [Test]
    public async Task ExplicitRecovering()
    {
      var count = 100;

      using (var dic1 = new Dictionary<string, string>(_folder, new StringSerializer(), new StringSerializer()))
      {
        for (int i = 0; i < count; i++)
        {
          await dic1.Write($"KEY {i}", $"VALUE {i}");
        }

        await dic1.Backup();
      }

      using (var dic2 = new Dictionary<string, string>(_folder, new StringSerializer(), new StringSerializer()))
      {
        await dic2.Restore();

        Assert.That(count == await dic2.Count());

        for (int i = 0; i < count; i++)
        {
          Assert.That($"VALUE {i}" == await dic2.Read($"KEY {i}"));
        }
      }

      Directory.Delete(_folder, true);
    }
  }
}
