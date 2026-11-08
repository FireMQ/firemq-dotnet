using System.Threading.Channels;
using FireMQ.Protocol;

namespace FireMQ;

/// <summary>
/// A stream of messages from a queue. Enumerate it with <c>await foreach</c>, or call
/// <see cref="NextAsync"/>. The enumeration ends after <see cref="CancelAsync"/>, when the
/// client closes, or when the broker cancels the subscription (for example because the
/// queue was deleted). It throws <see cref="ConnectionClosedException"/> when the connection
/// is lost.
/// </summary>
/// <remarks>
/// The broker sends at most <see cref="SubscribeOptions.Prefetch"/> messages ahead. Credit
/// is returned in batches of half that window as messages are acknowledged or rejected (or,
/// with no-ack, as they are read), so an unacknowledged message holds its slot.
/// </remarks>
public sealed class Subscription : IAsyncEnumerable<Message>, IAsyncDisposable
{
    private readonly FireMQClient _client;
    private readonly Channel<Message> _messages;
    private readonly uint _window;
    private readonly bool _noAck;
    private int _released;
    private int _cancelled;

    internal Subscription(FireMQClient client, uint id, string queue, uint window, bool noAck)
    {
        _client = client;
        Id = id;
        Queue = queue;
        _window = window;
        _noAck = noAck;
        _messages = Channel.CreateUnbounded<Message>(new UnboundedChannelOptions
        {
            SingleWriter = true,
            SingleReader = false,
            AllowSynchronousContinuations = false,
        });
    }

    /// <summary>The subscription id on this connection.</summary>
    public uint Id { get; }

    internal bool NoAck => _noAck;

    /// <summary>The queue this subscription consumes from.</summary>
    public string Queue { get; }

    /// <summary>The reply code when the broker cancelled the subscription, for example 404 when the queue was deleted.</summary>
    public int? CancelCode { get; private set; }

    /// <summary>The reason the broker gave when it cancelled the subscription.</summary>
    public string? CancelReason { get; private set; }

    /// <summary>
    /// Waits for the next message. Returns null once the subscription has ended and every
    /// message already received has been read.
    /// </summary>
    /// <exception cref="ConnectionClosedException">The connection was lost.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled. No message is lost.</exception>
    public async ValueTask<Message?> NextAsync(CancellationToken cancellationToken = default)
    {
        var reader = _messages.Reader;
        while (true)
        {
            if (reader.TryRead(out var message))
            {
                if (_noAck)
                {
                    Release(1);
                }
                return message;
            }
            if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return null;
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<Message> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        while (await NextAsync(cancellationToken).ConfigureAwait(false) is { } message)
        {
            yield return message;
        }
    }

    /// <summary>
    /// Stops the subscription and waits for the broker to confirm. Messages already received
    /// can still be read and acknowledged; unacknowledged ones return to the queue when the
    /// connection closes.
    /// </summary>
    public async Task CancelAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _cancelled, 1) != 0)
        {
            return;
        }
        try
        {
            await _client.UnsubscribeAsync(Id, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _client.RemoveSubscription(Id);
            _messages.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Cancels the subscription. Waits at most five seconds for the broker, and ignores errors
    /// from a closed connection.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try
        {
            await CancelAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (Exception e) when (e is FireMQException or OperationCanceledException)
        {
        }
    }

    internal void Deliver(Message message) => _messages.Writer.TryWrite(message);

    internal void Complete(Exception? error) => _messages.Writer.TryComplete(error);

    internal void Cancelled(int code, string reason)
    {
        CancelCode = code;
        CancelReason = reason;
        Interlocked.Exchange(ref _cancelled, 1);
        _messages.Writer.TryComplete();
    }

    /// <summary>Records handled messages and grants credit back in batches of half the window.</summary>
    internal void Release(int count)
    {
        int total = Interlocked.Add(ref _released, count);
        int batch = (int)Math.Max(1, _window / 2);
        if (total < batch)
        {
            return;
        }
        int granted = Interlocked.Exchange(ref _released, 0);
        if (granted > 0 && Volatile.Read(ref _cancelled) == 0)
        {
            _client.TrySend(new CreditFrame { Sub = Id, Credit = (uint)granted });
        }
    }
}
