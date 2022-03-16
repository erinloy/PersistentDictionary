using FASTER.core;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PersistentDictionary
{
  public class Dictionary<TKEY, TVALUE> : IDisposable
  {
    private FasterKV<TKEY, TVALUE> _store;
    private readonly SimpleFunctions<TKEY, TVALUE> _functions;
    
    readonly string _path;

    readonly string _logDevicePath;
    private IDevice _log;

    readonly string _logObjectDevicePath;
    private IDevice _objLog;

    private bool _disposedValue;

    public Dictionary(string storagePath, BinaryObjectSerializer<TKEY> keySerializer, BinaryObjectSerializer<TVALUE> valueSerializer)
    {
      _path = Path.GetFullPath(storagePath);

      _logDevicePath = Path.GetFullPath(Path.Combine(storagePath, "hlog.log"));
      _log = Devices.CreateLogDevice(_logDevicePath);
      
      _logObjectDevicePath = Path.GetFullPath(Path.Combine(storagePath, "hlog.obj.log"));
      _objLog = Devices.CreateLogDevice(_logObjectDevicePath);
      
      var serializerSettings = new SerializerSettings<TKEY, TVALUE>
      {
          keySerializer = () => keySerializer,
          valueSerializer = () => valueSerializer
      };

      _store = new FasterKV<TKEY, TVALUE>(
          1L << 20,
          new LogSettings { LogDevice = _log, ObjectLogDevice = _objLog },
          new CheckpointSettings { CheckpointDir = storagePath },
          serializerSettings: serializerSettings
          );

      _functions = new SimpleFunctions<TKEY, TVALUE>();
    }

    public Task<long> Count(CancellationToken cancellationToken = default)
    {
      using var s = _store.NewSession(_functions);
      var iterator = s.Iterate();
      long count = 0;
      
      while (iterator.GetNext(out var _))
      {
        count++;
        cancellationToken.ThrowIfCancellationRequested();
      }
     
      return Task.FromResult(count);
    }

    public async Task Iterate(Func<TKEY,TVALUE, Task> cursor, CancellationToken cancellationToken = default)
    {
      using var s = _store.NewSession(_functions);
      var iterator = s.Iterate();

      while (iterator.GetNext(out var _))
      {
        await cursor(iterator.GetKey(),iterator.GetValue());

        cancellationToken.ThrowIfCancellationRequested();
      }
    }

    public async Task Backup() 
    {
        // Take index + fold-over checkpoint of FASTER, wait to complete
        await _store.TakeFullCheckpointAsync(CheckpointType.FoldOver);
    } 

    public async Task Write(TKEY key, TVALUE value)
    {
      using var s = _store.NewSession(_functions);
      
      var operation = await s.UpsertAsync(ref key, ref value);

      do
      {
        operation = await operation.CompleteAsync();
      }
      while (!operation.Status.IsCompleted);
    }

    public async Task Restore()
    {
      if (Directory.Exists(_path))
        await _store.RecoverAsync();
    }

    public async Task<TVALUE> Read(TKEY key)
    {
      using var s = _store.NewSession(_functions);
      var (status, output) = (await s.ReadAsync(ref key)).Complete();

      return output;
    }

    protected virtual void Dispose(bool disposing)
    {
      if (_disposedValue) return;

      if (disposing)
      {
        // Dispose store instance
        _store.Dispose();

        // Close logs
        _log.Dispose();
        _objLog.Dispose();
      }

      // TODO: set large fields to null
      _disposedValue = true;
      _store = null;
      _log = null;
      _objLog = null;
    }

    public void Dispose()
    {
      // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
      Dispose(disposing: true);
      GC.SuppressFinalize(this);
    }
  }
}
