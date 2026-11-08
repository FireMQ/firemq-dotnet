using System.Buffers.Binary;
using System.Diagnostics;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using FireMQ.Protocol;

namespace FireMQ;

/// <summary>
/// A connection to FireMQ. Thread-safe: share one client across the application. Dispose it
/// to close the connection.
/// </summary>
/// <remarks>
/// Defaults favour safety: queues and exchanges are durable, messages are persistent, and
/// <see cref="PublishAsync(string, string, ReadOnlyMemory{byte}, PublishOptions?, CancellationToken)"/>
/// completes when the broker confirms the message.
/// </remarks>
public sealed class FireMQClient : IAsyncDisposable
{
    private const int WriteBatchSize = 64 * 1024;
    private static readonly PublishOptions DefaultPublishOptions = new();

    private readonly object _lock = new();
    private readonly object _publishLock = new();
    private readonly Stream _stream;
    private readonly Stream _readStream;
    private readonly Channel<byte[]> _outbox;
    private readonly Dictionary<uint, TaskCompletionSource<Frame>> _requests = new();
    private readonly SortedDictionary<ulong, TaskCompletionSource<bool>> _confirms = new();
    private readonly Dictionary<ulong, ReturnFrame> _returned = new();
    private readonly Dictionary<uint, Subscription> _subscriptions = new();
    private readonly TaskCompletionSource<bool> _closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly CancellationTokenSource _heartbeatStop = new();
    private readonly uint _maxFrame;
    private readonly uint _maxIncomingFrame;
    private readonly Task _readLoop;
    private readonly Task _writeLoop;
    private readonly Task _heartbeatLoop;

    private ConnectionClosedException? _closeReason;
    private TaskCompletionSource<bool>? _flowResumed;
    private int _closeRequested;
    private int _closeSent;
    private int _transportDisposed;
    private int _nextRid;
    private int _nextSub;
    private ulong _lastSeq;
    private long _lastSend;
    private long _lastReceive;

    private FireMQClient(Stream stream, Stream readStream, WelcomeFrame welcome)
    {
        _stream = stream;
        _readStream = readStream;
        _maxFrame = welcome.MaxFrame == 0 ? Proto.DefaultMaxFrame : welcome.MaxFrame;
        _maxIncomingFrame = Math.Max(_maxFrame, Proto.MaxHelloFrame) + 64 * 1024;
        Server = new ServerInfo(
            welcome.ServerName,
            welcome.ServerVersion,
            welcome.ConnId,
            TimeSpan.FromSeconds(welcome.Heartbeat),
            _maxFrame,
            welcome.Caps.AsReadOnly());
        _outbox = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        _lastSend = _lastReceive = Stopwatch.GetTimestamp();
        _writeLoop = Task.Run(WriteLoopAsync);
        _readLoop = Task.Run(ReadLoopAsync);
        _heartbeatLoop = welcome.Heartbeat > 0 ? Task.Run(() => HeartbeatLoopAsync(welcome.Heartbeat)) : Task.CompletedTask;
    }

    /// <summary>The SDK version sent to the broker.</summary>
    public static string Version { get; } = ReadVersion();

    /// <summary>Server name, version and limits from the handshake.</summary>
    public ServerInfo Server { get; }

    /// <summary>True once the connection is closed, by either side or by a network failure.</summary>
    public bool IsClosed
    {
        get
        {
            lock (_lock)
            {
                return _closeReason is not null;
            }
        }
    }

    /// <summary>Why the connection closed, or null while it is open.</summary>
    public ConnectionClosedException? CloseReason
    {
        get
        {
            lock (_lock)
            {
                return _closeReason;
            }
        }
    }

    /// <summary>A task that completes when the connection closes.</summary>
    public Task Completion => _closed.Task;

    /// <summary>
    /// Connects with a URL such as <c>fire://user:pass@host:5673/vhost</c>. User and password
    /// default to <c>guest</c>, the port to 5673 (5674 for <c>fires://</c>), and the vhost to <c>/</c>.
    /// </summary>
    /// <exception cref="ServerException">The broker refused the connection, for example with 403 for bad credentials.</exception>
    /// <exception cref="FireMQException">The broker could not be reached, or the handshake failed or timed out.</exception>
    public static async Task<FireMQClient> ConnectAsync(string url, ConnectOptions? options = null, CancellationToken cancellationToken = default)
    {
        var target = ConnectUrl.Parse(url);
        options ??= new ConnectOptions();
        if (target.Transport is Transport.Ws or Transport.Wss)
        {
            throw new NotSupportedException("FireMQ.Client connects with fire:// or fires://; WebSocket URLs are meant for browsers");
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (options.ConnectTimeout > TimeSpan.Zero)
        {
            timeout.CancelAfter(options.ConnectTimeout);
        }
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        Stream? stream = null;
        Stream readStream;
        WelcomeFrame welcome;
        try
        {
            // Disposing the socket is the one way to abort every pending operation on it.
            using (timeout.Token.Register(() => socket.Dispose()))
            {
                await socket.ConnectAsync(target.Host, target.Port).ConfigureAwait(false);
                stream = new NetworkStream(socket, ownsSocket: true);
                if (target.Transport == Transport.Tls)
                {
                    stream = await Tls.AuthenticateAsync(stream, target, options, timeout.Token).ConfigureAwait(false);
                }
                readStream = new BufferedStream(stream, 64 * 1024);

                var hello = new HelloFrame
                {
                    Ver = Proto.Version,
                    Vhost = target.Vhost,
                    Auth = new AuthInfo { Mech = "plain", User = target.User, Pass = target.Password },
                    Heartbeat = HeartbeatSeconds(options.Heartbeat),
                    Client = ClientInfo(options),
                    Caps = new List<string> { "confirms", "credit" },
                }.Encode();
                var first = new byte[Proto.Magic.Length + hello.Length];
                Proto.Magic.CopyTo(first);
                hello.CopyTo(first.AsSpan(Proto.Magic.Length));
                await stream.WriteAsync(first, 0, first.Length, timeout.Token).ConfigureAwait(false);
                await stream.FlushAsync(timeout.Token).ConfigureAwait(false);

                var reply = Frame.Decode(await ReadFrameAsync(readStream, new byte[4], Proto.MaxHelloFrame, timeout.Token).ConfigureAwait(false));
                switch (reply)
                {
                    case WelcomeFrame w:
                        welcome = w;
                        break;
                    case ErrorFrame error:
                        throw new ServerException(error.Code, error.Msg);
                    case CloseFrame close:
                        throw new ConnectionClosedException($"the broker closed the connection during the handshake: {close.Msg} (code {close.Code})", close.Code);
                    default:
                        throw new ProtocolException($"expected welcome, got {reply?.OpName ?? "an unknown op"}");
                }
            }
        }
        catch (Exception e)
        {
            stream?.Dispose();
            socket.Dispose();
            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            if (timeout.IsCancellationRequested)
            {
                throw new FireMQException($"connecting to {target.Authority} timed out after {options.ConnectTimeout.TotalSeconds:0.#} s", e);
            }
            switch (e)
            {
                case FireMQException:
                    throw;
                case SocketException:
                    throw new FireMQException($"could not connect to {target.Authority}: {e.Message}", e);
                case EndOfStreamException:
                    throw new ConnectionClosedException($"the broker at {target.Authority} closed the connection during the handshake", null, e);
                case IOException or AuthenticationException:
                    throw new FireMQException($"connecting to {target.Authority} failed: {e.Message}", e);
                default:
                    throw;
            }
        }
        return new FireMQClient(stream, readStream, welcome);
    }

    /// <summary>Declares a queue. An empty name asks the broker to generate one; read it from <see cref="QueueInfo.Name"/>.</summary>
    public async Task<QueueInfo> DeclareQueueAsync(string name, QueueOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new QueueOptions();
        var ok = await CallOkAsync(rid => new DeclareQueueFrame
        {
            Rid = rid,
            Name = name ?? "",
            Durable = options.Durable,
            Exclusive = options.Exclusive,
            AutoDelete = options.AutoDelete,
            Passive = options.Passive,
            Args = options.Arguments,
        }, cancellationToken).ConfigureAwait(false);
        return new QueueInfo(ok.QueueName ?? name ?? "", ok.Messages ?? 0, ok.Consumers ?? 0);
    }

    /// <summary>Deletes a queue and returns how many messages it held.</summary>
    /// <param name="name">The queue.</param>
    /// <param name="ifUnused">Fail with 406 when the queue has subscriptions.</param>
    /// <param name="ifEmpty">Fail with 406 when the queue has messages.</param>
    /// <param name="cancellationToken">Stops waiting for the reply.</param>
    public async Task<ulong> DeleteQueueAsync(string name, bool ifUnused = false, bool ifEmpty = false, CancellationToken cancellationToken = default)
    {
        var ok = await CallOkAsync(rid => new DeleteQueueFrame { Rid = rid, Name = Required(name, nameof(name)), IfUnused = ifUnused, IfEmpty = ifEmpty }, cancellationToken).ConfigureAwait(false);
        return ok.Messages ?? 0;
    }

    /// <summary>Removes every ready message from a queue and returns how many were removed.</summary>
    public async Task<ulong> PurgeQueueAsync(string name, CancellationToken cancellationToken = default)
    {
        var ok = await CallOkAsync(rid => new PurgeQueueFrame { Rid = rid, Name = Required(name, nameof(name)) }, cancellationToken).ConfigureAwait(false);
        return ok.Messages ?? 0;
    }

    /// <summary>Declares an exchange of type <c>direct</c>, <c>fanout</c>, <c>topic</c> or <c>headers</c> (see <see cref="ExchangeType"/>).</summary>
    public Task DeclareExchangeAsync(string name, string type, ExchangeOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new ExchangeOptions();
        return CallOkAsync(rid => new DeclareExchangeFrame
        {
            Rid = rid,
            Name = Required(name, nameof(name)),
            Type = Required(type, nameof(type)),
            Durable = options.Durable,
            AutoDelete = options.AutoDelete,
            Internal = options.Internal,
            Passive = options.Passive,
            Args = options.Arguments,
        }, cancellationToken);
    }

    /// <summary>Deletes an exchange.</summary>
    /// <param name="name">The exchange.</param>
    /// <param name="ifUnused">Fail with 406 when the exchange has bindings.</param>
    /// <param name="cancellationToken">Stops waiting for the reply.</param>
    public Task DeleteExchangeAsync(string name, bool ifUnused = false, CancellationToken cancellationToken = default) =>
        CallOkAsync(rid => new DeleteExchangeFrame { Rid = rid, Name = Required(name, nameof(name)), IfUnused = ifUnused }, cancellationToken);

    /// <summary>Binds <paramref name="queue"/> to <paramref name="exchange"/> with a routing key or pattern.</summary>
    /// <param name="queue">The destination queue.</param>
    /// <param name="exchange">The source exchange.</param>
    /// <param name="routingKey">The binding key; a pattern for topic exchanges.</param>
    /// <param name="arguments">Binding arguments, for example <c>x-match</c> for headers exchanges.</param>
    /// <param name="cancellationToken">Stops waiting for the reply.</param>
    public Task BindQueueAsync(string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default) =>
        CallOkAsync(rid => new BindFrame { Rid = rid, Queue = Required(queue, nameof(queue)), Exchange = exchange ?? "", Key = routingKey ?? "", Args = arguments }, cancellationToken);

    /// <summary>Removes a binding made with <see cref="BindQueueAsync"/>.</summary>
    public Task UnbindQueueAsync(string queue, string exchange, string routingKey, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default) =>
        CallOkAsync(rid => new BindFrame { Unbind = true, Rid = rid, Queue = Required(queue, nameof(queue)), Exchange = exchange ?? "", Key = routingKey ?? "", Args = arguments }, cancellationToken);

    /// <summary>Binds exchange <paramref name="destination"/> to exchange <paramref name="source"/>: messages that reach <paramref name="source"/> with a matching key also go to <paramref name="destination"/>.</summary>
    public Task BindExchangeAsync(string destination, string source, string routingKey, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default) =>
        CallOkAsync(rid => new BindFrame { Rid = rid, ToExchange = Required(destination, nameof(destination)), Exchange = Required(source, nameof(source)), Key = routingKey ?? "", Args = arguments }, cancellationToken);

    /// <summary>Removes a binding made with <see cref="BindExchangeAsync"/>.</summary>
    public Task UnbindExchangeAsync(string destination, string source, string routingKey, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default) =>
        CallOkAsync(rid => new BindFrame { Unbind = true, Rid = rid, ToExchange = Required(destination, nameof(destination)), Exchange = Required(source, nameof(source)), Key = routingKey ?? "", Args = arguments }, cancellationToken);

    /// <summary>
    /// Publishes a message and, by default, waits for the broker's confirm. Use exchange
    /// <c>""</c> to send straight to the queue named by <paramref name="routingKey"/>.
    /// </summary>
    /// <exception cref="UnroutableException">The publish was mandatory and no queue received it.</exception>
    /// <exception cref="NackedException">The broker rejected the message, for example because a queue is full.</exception>
    /// <exception cref="ConnectionClosedException">The connection closed before the confirm arrived; the message may or may not have been delivered.</exception>
    public async Task PublishAsync(string exchange, string routingKey, ReadOnlyMemory<byte> body, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= DefaultPublishOptions;
        cancellationToken.ThrowIfCancellationRequested();
        var paused = Volatile.Read(ref _flowResumed);
        if (paused is not null)
        {
            await WaitAsync(paused.Task, cancellationToken).ConfigureAwait(false);
        }

        var frame = new PublishFrame
        {
            Exchange = exchange ?? "",
            Key = routingKey ?? "",
            Mandatory = options.Mandatory,
            Persistent = options.Persistent,
            Props = options.Properties,
            Headers = options.Headers,
            Body = body,
        };
        TaskCompletionSource<bool>? confirm = null;
        ulong seq = 0;
        lock (_publishLock)
        {
            // Sequence numbers must reach the broker in order and without gaps, so a number
            // is only taken once the frame is encoded and queued.
            if (options.Confirm)
            {
                seq = _lastSeq + 1;
                frame.Seq = seq;
            }
            byte[] bytes = frame.Encode();
            if ((ulong)bytes.Length - 4 > _maxFrame)
            {
                throw new ArgumentException($"a message of {body.Length} bytes does not fit in the broker's frame limit of {_maxFrame} bytes", nameof(body));
            }
            lock (_lock)
            {
                ThrowIfClosed();
                if (seq != 0)
                {
                    confirm = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _confirms[seq] = confirm;
                }
                if (!_outbox.Writer.TryWrite(bytes))
                {
                    _confirms.Remove(seq);
                    throw ClosedError();
                }
            }
            if (seq != 0)
            {
                _lastSeq = seq;
            }
        }
        if (confirm is null)
        {
            return;
        }
        if (!cancellationToken.CanBeCanceled)
        {
            await confirm.Task.ConfigureAwait(false);
            return;
        }
        using (cancellationToken.Register(() =>
        {
            lock (_lock)
            {
                _confirms.Remove(seq);
            }
            confirm.TrySetCanceled(cancellationToken);
        }))
        {
            await confirm.Task.ConfigureAwait(false);
        }
    }

    /// <summary>Publishes a UTF-8 text message. See <see cref="PublishAsync(string, string, ReadOnlyMemory{byte}, PublishOptions?, CancellationToken)"/>.</summary>
    public Task PublishAsync(string exchange, string routingKey, string body, PublishOptions? options = null, CancellationToken cancellationToken = default) =>
        PublishAsync(exchange, routingKey, Encoding.UTF8.GetBytes(body ?? ""), options, cancellationToken);

    /// <summary>
    /// Publishes <paramref name="value"/> serialized as JSON, with content type
    /// <c>application/json</c> unless <paramref name="options"/> sets another.
    /// </summary>
    public Task PublishJsonAsync<T>(string exchange, string routingKey, T value, PublishOptions? options = null, JsonSerializerOptions? jsonOptions = null, CancellationToken cancellationToken = default)
    {
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(value, jsonOptions);
        var source = options ?? DefaultPublishOptions;
        var properties = source.Properties.Clone();
        properties.ContentType ??= "application/json";
        var effective = new PublishOptions
        {
            Persistent = source.Persistent,
            Mandatory = source.Mandatory,
            Confirm = source.Confirm,
            Headers = source.Headers,
            Properties = properties,
        };
        return PublishAsync(exchange, routingKey, body, effective, cancellationToken);
    }

    /// <summary>Starts consuming from a queue.</summary>
    public async Task<Subscription> SubscribeAsync(string queue, SubscribeOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new SubscribeOptions();
        Required(queue, nameof(queue));
        uint id = (uint)Interlocked.Increment(ref _nextSub);
        uint window = (uint)Math.Max(1, options.Prefetch);
        var subscription = new Subscription(this, id, queue, window, options.NoAck);
        lock (_lock)
        {
            ThrowIfClosed();
            _subscriptions[id] = subscription;
        }
        try
        {
            await CallOkAsync(rid => new SubscribeFrame
            {
                Rid = rid,
                Sub = id,
                Queue = queue,
                Credit = window,
                NoAck = options.NoAck,
                Exclusive = options.Exclusive,
                Tag = options.Tag ?? "",
                Args = options.Arguments,
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            RemoveSubscription(id);
            if (e is OperationCanceledException)
            {
                // The broker may still create the subscription; end it.
                TrySend(new UnsubscribeFrame { Rid = NextRid(), Sub = id });
            }
            throw;
        }
        return subscription;
    }

    /// <summary>Takes one message from a queue, or returns null when the queue is empty.</summary>
    /// <param name="queue">The queue.</param>
    /// <param name="noAck">Treat the message as acknowledged once sent.</param>
    /// <param name="cancellationToken">Stops waiting for the reply.</param>
    public async Task<Message?> GetAsync(string queue, bool noAck = false, CancellationToken cancellationToken = default)
    {
        var reply = await CallAsync(rid => new GetFrame { Rid = rid, Queue = Required(queue, nameof(queue)), NoAck = noAck }, cancellationToken).ConfigureAwait(false);
        return reply switch
        {
            GetOkFrame g => new Message(noAck ? null : this, null, g.Tag, g.Redelivered, g.Exchange, g.Key, g.Props, g.Headers, g.Body),
            OkFrame => null,
            _ => throw new ProtocolException($"unexpected {reply.OpName} reply to get"),
        };
    }

    /// <summary>
    /// Closes the connection gracefully. Pending requests fail with
    /// <see cref="ConnectionClosedException"/> and subscriptions end. Unacknowledged messages
    /// return to their queues.
    /// </summary>
    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (_closeReason is null)
            {
                Volatile.Write(ref _closeRequested, 1);
            }
        }
        if (Volatile.Read(ref _closeRequested) == 1 && Interlocked.Exchange(ref _closeSent, 1) == 0)
        {
            TrySend(new CloseFrame { Code = 200, Msg = "client closed" });
            await Task.WhenAny(_closed.Task, Task.Delay(TimeSpan.FromSeconds(2), cancellationToken)).ConfigureAwait(false);
        }
        Shutdown(new ConnectionClosedException("the client closed the connection", 200));
        await Task.WhenAny(Task.WhenAll(_writeLoop, _readLoop, _heartbeatLoop), Task.Delay(TimeSpan.FromSeconds(2), cancellationToken)).ConfigureAwait(false);
        DisposeTransport();
    }

    /// <summary>Closes the connection. See <see cref="CloseAsync"/>.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await CloseAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            DisposeTransport();
        }
    }

    // ---- Internals shared with Message and Subscription ----

    internal void Send(Frame frame)
    {
        if (!_outbox.Writer.TryWrite(frame.Encode()))
        {
            throw ClosedError();
        }
    }

    internal bool TrySend(Frame frame) => _outbox.Writer.TryWrite(frame.Encode());

    internal Task UnsubscribeAsync(uint id, CancellationToken cancellationToken) =>
        CallOkAsync(rid => new UnsubscribeFrame { Rid = rid, Sub = id }, cancellationToken);

    internal void RemoveSubscription(uint id)
    {
        lock (_lock)
        {
            _subscriptions.Remove(id);
        }
    }

    private uint NextRid()
    {
        uint rid;
        do
        {
            rid = (uint)Interlocked.Increment(ref _nextRid);
        }
        while (rid == 0);
        return rid;
    }

    private async Task<OkFrame> CallOkAsync(Func<uint, Frame> build, CancellationToken cancellationToken)
    {
        var reply = await CallAsync(build, cancellationToken).ConfigureAwait(false);
        return reply as OkFrame ?? throw new ProtocolException($"unexpected {reply.OpName} reply");
    }

    private async Task<Frame> CallAsync(Func<uint, Frame> build, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        uint rid = NextRid();
        byte[] bytes = build(rid).Encode();
        var reply = new TaskCompletionSource<Frame>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            ThrowIfClosed();
            _requests[rid] = reply;
            if (!_outbox.Writer.TryWrite(bytes))
            {
                _requests.Remove(rid);
                throw ClosedError();
            }
        }
        if (!cancellationToken.CanBeCanceled)
        {
            return await reply.Task.ConfigureAwait(false);
        }
        using (cancellationToken.Register(() =>
        {
            lock (_lock)
            {
                _requests.Remove(rid);
            }
            reply.TrySetCanceled(cancellationToken);
        }))
        {
            return await reply.Task.ConfigureAwait(false);
        }
    }

    private void ThrowIfClosed()
    {
        if (_closeReason is not null)
        {
            throw ClosedError();
        }
    }

    private ConnectionClosedException ClosedError()
    {
        var reason = _closeReason;
        return reason is null
            ? new ConnectionClosedException("the connection is closed")
            : new ConnectionClosedException(reason.Message, reason.Code, reason.InnerException);
    }

    // ---- Read loop ----

    private async Task ReadLoopAsync()
    {
        ConnectionClosedException? reason = null;
        var lengthBuffer = new byte[4];
        try
        {
            while (reason is null)
            {
                var raw = await ReadFrameAsync(_readStream, lengthBuffer, _maxIncomingFrame, CancellationToken.None).ConfigureAwait(false);
                Volatile.Write(ref _lastReceive, Stopwatch.GetTimestamp());
                Frame? frame;
                try
                {
                    frame = Frame.Decode(raw);
                }
                catch (ProtocolException)
                {
                    // A header this client cannot decode; skip the frame as the Rust SDK does.
                    continue;
                }
                if (frame is not null)
                {
                    reason = Dispatch(frame);
                }
            }
        }
        catch (EndOfStreamException)
        {
            reason = new ConnectionClosedException("the broker closed the connection");
        }
        catch (ProtocolException e)
        {
            reason = new ConnectionClosedException($"protocol error: {e.Message}", null, e);
        }
        catch (Exception e)
        {
            reason = new ConnectionClosedException($"connection lost: {e.Message}", null, e);
        }
        Shutdown(reason);
    }

    /// <summary>Handles one frame from the broker. Returns a reason when the connection is over.</summary>
    private ConnectionClosedException? Dispatch(Frame frame)
    {
        switch (frame)
        {
            case OkFrame ok:
                CompleteRequest(ok.Rid, ok, null);
                break;
            case GetOkFrame getOk:
                CompleteRequest(getOk.Rid, getOk, null);
                break;
            case ErrorFrame error:
                if (error.Rid != 0)
                {
                    CompleteRequest(error.Rid, null, new ServerException(error.Code, error.Msg));
                }
                if (error.Fatal)
                {
                    return new ConnectionClosedException($"the broker closed the connection: {error.Msg} (code {error.Code})", error.Code);
                }
                break;
            case ConfirmFrame confirm:
                HandleConfirm(confirm);
                break;
            case ReturnFrame returned:
                if (returned.Seq != 0)
                {
                    lock (_lock)
                    {
                        _returned[returned.Seq] = returned;
                    }
                }
                break;
            case DeliverFrame deliver:
                HandleDeliver(deliver);
                break;
            case CancelledFrame cancelled:
            {
                Subscription? subscription;
                lock (_lock)
                {
                    if (_subscriptions.TryGetValue(cancelled.Sub, out subscription))
                    {
                        _subscriptions.Remove(cancelled.Sub);
                    }
                }
                subscription?.Cancelled(cancelled.Code, cancelled.Msg);
                break;
            }
            case PingFrame:
                TrySend(PongFrame.Instance);
                break;
            case FlowFrame flow:
                SetFlow(flow.Pause);
                break;
            case CloseFrame close:
                if (Interlocked.Exchange(ref _closeSent, 1) == 0)
                {
                    TrySend(new CloseFrame { Code = 200 });
                }
                return new ConnectionClosedException(
                    close.Msg.Length == 0
                        ? $"the broker closed the connection (code {close.Code})"
                        : $"the broker closed the connection: {close.Msg} (code {close.Code})",
                    close.Code);
        }
        return null;
    }

    private void CompleteRequest(uint rid, Frame? reply, Exception? error)
    {
        TaskCompletionSource<Frame>? pending;
        lock (_lock)
        {
            if (!_requests.TryGetValue(rid, out pending))
            {
                return;
            }
            _requests.Remove(rid);
        }
        if (error is not null)
        {
            pending.TrySetException(error);
        }
        else
        {
            pending.TrySetResult(reply!);
        }
    }

    private void HandleConfirm(ConfirmFrame confirm)
    {
        var done = new List<(TaskCompletionSource<bool> Pending, Exception? Error)>();
        lock (_lock)
        {
            if (confirm.Nack)
            {
                if (confirm.Seq != 0)
                {
                    _returned.Remove(confirm.Seq);
                    if (_confirms.TryGetValue(confirm.Seq, out var pending))
                    {
                        _confirms.Remove(confirm.Seq);
                        done.Add((pending, new NackedException(confirm.Code, confirm.Msg)));
                    }
                }
            }
            else if (confirm.Upto != 0)
            {
                // Confirms are cumulative: every outstanding publish up to `upto` is settled.
                // A mandatory publish the broker returned fails; its return came first.
                var settled = new List<ulong>();
                foreach (var entry in _confirms)
                {
                    if (entry.Key > confirm.Upto)
                    {
                        break;
                    }
                    settled.Add(entry.Key);
                    Exception? error = null;
                    if (_returned.TryGetValue(entry.Key, out var returned))
                    {
                        _returned.Remove(entry.Key);
                        error = new UnroutableException(returned.Exchange, returned.Key, returned.Code, returned.Msg);
                    }
                    done.Add((entry.Value, error));
                }
                foreach (ulong seq in settled)
                {
                    _confirms.Remove(seq);
                }
                if (_returned.Count > 0)
                {
                    foreach (ulong stale in _returned.Keys.Where(k => k <= confirm.Upto).ToList())
                    {
                        _returned.Remove(stale);
                    }
                }
            }
        }
        foreach (var (pending, error) in done)
        {
            if (error is null)
            {
                pending.TrySetResult(true);
            }
            else
            {
                pending.TrySetException(error);
            }
        }
    }

    private void HandleDeliver(DeliverFrame deliver)
    {
        Subscription? subscription;
        lock (_lock)
        {
            _subscriptions.TryGetValue(deliver.Sub, out subscription);
        }
        if (subscription is null)
        {
            return;
        }
        var acker = subscription.NoAck ? null : this;
        subscription.Deliver(new Message(acker, subscription, deliver.Tag, deliver.Redelivered, deliver.Exchange, deliver.Key, deliver.Props, deliver.Headers, deliver.Body));
    }

    private void SetFlow(bool pause)
    {
        TaskCompletionSource<bool>? resumed = null;
        lock (_lock)
        {
            if (pause)
            {
                _flowResumed ??= new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            else
            {
                resumed = _flowResumed;
                _flowResumed = null;
            }
        }
        resumed?.TrySetResult(true);
    }

    private static async Task<RawFrame> ReadFrameAsync(Stream stream, byte[] lengthBuffer, uint maxFrame, CancellationToken cancellationToken)
    {
        await ReadExactAsync(stream, lengthBuffer, 4, cancellationToken).ConfigureAwait(false);
        uint length = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);
        RawFrame.CheckLength(length, maxFrame);
        var frame = new byte[length];
        await ReadExactAsync(stream, frame, (int)length, cancellationToken).ConfigureAwait(false);
        return RawFrame.Parse(frame);
    }

    private static async Task ReadExactAsync(Stream stream, byte[] buffer, int count, CancellationToken cancellationToken)
    {
        int read = 0;
        while (read < count)
        {
            int n = await stream.ReadAsync(buffer, read, count - read, cancellationToken).ConfigureAwait(false);
            if (n == 0)
            {
                throw new EndOfStreamException();
            }
            read += n;
        }
    }

    // ---- Write loop ----

    private async Task WriteLoopAsync()
    {
        var reader = _outbox.Reader;
        var batch = new byte[WriteBatchSize];
        int length = 0;
        try
        {
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (reader.TryRead(out byte[]? frame))
                {
                    if (length + frame.Length > batch.Length)
                    {
                        if (length > 0)
                        {
                            await _stream.WriteAsync(batch, 0, length).ConfigureAwait(false);
                            length = 0;
                        }
                        if (frame.Length > batch.Length)
                        {
                            await _stream.WriteAsync(frame, 0, frame.Length).ConfigureAwait(false);
                            continue;
                        }
                    }
                    Buffer.BlockCopy(frame, 0, batch, length, frame.Length);
                    length += frame.Length;
                }
                if (length > 0)
                {
                    await _stream.WriteAsync(batch, 0, length).ConfigureAwait(false);
                    length = 0;
                }
                await _stream.FlushAsync().ConfigureAwait(false);
                Volatile.Write(ref _lastSend, Stopwatch.GetTimestamp());
            }
        }
        catch (Exception e)
        {
            Shutdown(new ConnectionClosedException($"connection lost: {e.Message}", null, e));
        }
        finally
        {
            DisposeTransport();
        }
    }

    // ---- Heartbeats ----

    private async Task HeartbeatLoopAsync(uint heartbeatSeconds)
    {
        var tick = TimeSpan.FromSeconds(Math.Max(1, heartbeatSeconds / 2.0));
        long tickTicks = (long)(tick.TotalSeconds * Stopwatch.Frequency);
        long deadTicks = (long)(heartbeatSeconds * 2.0 * Stopwatch.Frequency);
        try
        {
            while (true)
            {
                await Task.Delay(tick, _heartbeatStop.Token).ConfigureAwait(false);
                long now = Stopwatch.GetTimestamp();
                if (now - Volatile.Read(ref _lastSend) >= tickTicks)
                {
                    TrySend(PingFrame.Instance);
                }
                if (now - Volatile.Read(ref _lastReceive) > deadTicks)
                {
                    Shutdown(new ConnectionClosedException("the broker missed heartbeats"));
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // ---- Shutdown ----

    private void Shutdown(ConnectionClosedException reason)
    {
        List<TaskCompletionSource<Frame>> requests;
        List<TaskCompletionSource<bool>> confirms;
        List<Subscription> subscriptions;
        TaskCompletionSource<bool>? flow;
        bool clientClosed = Volatile.Read(ref _closeRequested) == 1;
        lock (_lock)
        {
            if (_closeReason is not null)
            {
                return;
            }
            if (clientClosed)
            {
                reason = new ConnectionClosedException("the client closed the connection", 200);
            }
            _closeReason = reason;
            requests = _requests.Values.ToList();
            _requests.Clear();
            confirms = _confirms.Values.ToList();
            _confirms.Clear();
            _returned.Clear();
            subscriptions = _subscriptions.Values.ToList();
            _subscriptions.Clear();
            flow = _flowResumed;
            _flowResumed = null;
            _outbox.Writer.TryComplete();
        }
        foreach (var request in requests)
        {
            request.TrySetException(ClosedError());
        }
        foreach (var confirm in confirms)
        {
            confirm.TrySetException(ClosedError());
        }
        foreach (var subscription in subscriptions)
        {
            subscription.Complete(clientClosed ? null : ClosedError());
        }
        flow?.TrySetException(ClosedError());
        _heartbeatStop.Cancel();
        _closed.TrySetResult(true);
    }

    private void DisposeTransport()
    {
        if (Interlocked.Exchange(ref _transportDisposed, 1) != 0)
        {
            return;
        }
        try
        {
            _stream.Dispose();
        }
        catch (Exception)
        {
        }
    }

    // ---- Helpers ----

    private static Dictionary<string, object?> ClientInfo(ConnectOptions options)
    {
        var info = new Dictionary<string, object?>
        {
            ["name"] = "firemq-dotnet",
            ["version"] = Version,
            ["lang"] = "dotnet",
        };
        if (!string.IsNullOrEmpty(options.ConnectionName))
        {
            info["connection_name"] = options.ConnectionName;
        }
        return info;
    }

    /// <summary>Whole seconds, rounded up so a short interval does not turn heartbeats off.</summary>
    private static uint HeartbeatSeconds(TimeSpan heartbeat) =>
        heartbeat <= TimeSpan.Zero ? 0 : (uint)Math.Min(Math.Ceiling(heartbeat.TotalSeconds), ushort.MaxValue);

    private static string ReadVersion()
    {
        string? version = typeof(FireMQClient).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (string.IsNullOrEmpty(version))
        {
            return typeof(FireMQClient).Assembly.GetName().Version?.ToString(3) ?? "0.0.0";
        }
        int plus = version!.IndexOf('+');
        return plus < 0 ? version : version.Substring(0, plus);
    }

    private static string Required(string value, string name) =>
        value ?? throw new ArgumentNullException(name);

    private static async Task WaitAsync(Task task, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            await task.ConfigureAwait(false);
            return;
        }
        var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (cancellationToken.Register(() => cancelled.TrySetCanceled(cancellationToken)))
        {
            await (await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false)).ConfigureAwait(false);
        }
    }
}
