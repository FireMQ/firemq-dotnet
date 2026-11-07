using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FireMQ.Protocol;

namespace FireMQ.Tests;

/// <summary>A scripted broker on a loopback socket, for testing the client's protocol logic.</summary>
internal sealed class FakeBroker : IDisposable
{
    private readonly TcpListener _listener;

    public FakeBroker()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
    }

    public string Url => $"fire://guest:guest@127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

    public async Task<FakeConnection> AcceptAsync()
    {
        var tcp = await _listener.AcceptTcpClientAsync().WaitAsync(TimeSpan.FromSeconds(5));
        var connection = new FakeConnection(tcp);
        byte[] magic = await connection.ReadBytesAsync(4);
        Assert.Equal("FMQ1"u8.ToArray(), magic);
        return connection;
    }

    /// <summary>Connects a client and completes the handshake.</summary>
    public async Task<(FireMQClient Client, FakeConnection Server, HelloFrame Hello)> ConnectAsync(
        uint heartbeat = 0, uint maxFrame = 1 << 20, ConnectOptions? options = null)
    {
        var connect = FireMQClient.ConnectAsync(Url, options ?? new ConnectOptions { Heartbeat = TimeSpan.Zero });
        var server = await AcceptAsync();
        var hello = Assert.IsType<HelloFrame>(await server.ReadAsync());
        await server.SendAsync(new WelcomeFrame
        {
            Ver = 1,
            ServerName = "fake",
            ServerVersion = "0.0.1",
            ConnId = "test-connection",
            Heartbeat = heartbeat,
            MaxFrame = maxFrame,
            Caps = new List<string> { "confirms", "credit" },
        });
        return (await connect.WaitAsync(TimeSpan.FromSeconds(5)), server, hello);
    }

    public void Dispose() => _listener.Stop();
}

internal sealed class FakeConnection : IAsyncDisposable
{
    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;

    public FakeConnection(TcpClient tcp)
    {
        _tcp = tcp;
        _stream = tcp.GetStream();
    }

    public async Task<byte[]> ReadBytesAsync(int count, TimeSpan? timeout = null)
    {
        var buffer = new byte[count];
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        await _stream.ReadExactlyAsync(buffer, cts.Token);
        return buffer;
    }

    /// <summary>Reads the next frame from the client.</summary>
    public async Task<Frame> ReadAsync(TimeSpan? timeout = null)
    {
        byte[] length = await ReadBytesAsync(4, timeout);
        byte[] body = await ReadBytesAsync((int)BinaryPrimitives.ReadUInt32BigEndian(length), timeout);
        return Frame.Decode(RawFrame.Parse(body)) ?? throw new InvalidOperationException("unknown op");
    }

    /// <summary>Reads the next frame, which must be a <typeparamref name="T"/>.</summary>
    public async Task<T> ExpectAsync<T>(TimeSpan? timeout = null) where T : Frame =>
        Assert.IsType<T>(await ReadAsync(timeout));

    /// <summary>True when the client sends nothing within <paramref name="window"/>.</summary>
    public async Task<bool> IsQuietAsync(TimeSpan window)
    {
        var read = ReadAsync(TimeSpan.FromSeconds(30));
        var winner = await Task.WhenAny(read, Task.Delay(window));
        if (winner == read)
        {
            await read;
            return false;
        }
        _pendingRead = read;
        return true;
    }

    private Task<Frame>? _pendingRead;

    /// <summary>Reads the next frame, continuing a read started by <see cref="IsQuietAsync"/>.</summary>
    public Task<Frame> NextAsync()
    {
        var pending = _pendingRead;
        _pendingRead = null;
        return pending ?? ReadAsync();
    }

    public async Task SendAsync(Frame frame)
    {
        await _stream.WriteAsync(frame.Encode());
        await _stream.FlushAsync();
    }

    public ValueTask DisposeAsync()
    {
        _tcp.Dispose();
        return ValueTask.CompletedTask;
    }
}
