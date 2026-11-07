using System.Collections.Concurrent;
using System.Globalization;

namespace FireMQ.Examples;

/// <summary>Tutorial 6: request and reply. The client sends <c>reply_to</c> and <c>correlation_id</c>; the server answers on that queue.</summary>
public static class Rpc
{
    private const string Queue = "rpc_queue.dotnet";

    public static async Task RunAsync(string url, CancellationToken ct)
    {
        await using var serverConnection = await FireMQClient.ConnectAsync(url, cancellationToken: ct);
        await serverConnection.DeclareQueueAsync(Queue, cancellationToken: ct);
        await serverConnection.PurgeQueueAsync(Queue, ct);
        var requests = await serverConnection.SubscribeAsync(Queue, new SubscribeOptions { Prefetch = 1 }, ct);
        var server = ServeAsync(serverConnection, requests, ct);

        await using (var client = await FibonacciClient.CreateAsync(url, ct))
        {
            foreach (int n in new[] { 1, 10, 30 })
            {
                Console.WriteLine($" [x] Requesting fib({n})");
                long result = await client.CallAsync(n, ct);
                Console.WriteLine($" [.] Got {result}");
            }
        }

        await requests.CancelAsync(ct);
        await server;
    }

    private static async Task ServeAsync(FireMQClient connection, Subscription requests, CancellationToken ct)
    {
        await foreach (var request in requests.WithCancellation(ct))
        {
            int n = int.Parse(request.GetString(), CultureInfo.InvariantCulture);
            Console.WriteLine($" [server] fib({n})");
            await connection.PublishAsync("", request.Properties.ReplyTo!, Fib(n).ToString(CultureInfo.InvariantCulture), new PublishOptions
            {
                Persistent = false,
                Properties = new MessageProperties { CorrelationId = request.Properties.CorrelationId },
            }, ct);
            request.Ack();
        }
    }

    private static long Fib(int n) => n < 2 ? n : Fib(n - 1) + Fib(n - 2);

    private sealed class FibonacciClient : IAsyncDisposable
    {
        private readonly FireMQClient _connection;
        private readonly string _replyQueue;
        private readonly Subscription _replies;
        private readonly Task _listener;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<string>> _pending = new();

        private FibonacciClient(FireMQClient connection, string replyQueue, Subscription replies)
        {
            _connection = connection;
            _replyQueue = replyQueue;
            _replies = replies;
            _listener = ListenAsync();
        }

        public static async Task<FibonacciClient> CreateAsync(string url, CancellationToken ct)
        {
            var connection = await FireMQClient.ConnectAsync(url, cancellationToken: ct);
            var replyQueue = await connection.DeclareQueueAsync("", QueueOptions.Temporary, ct);
            var replies = await connection.SubscribeAsync(replyQueue.Name, new SubscribeOptions { NoAck = true }, ct);
            return new FibonacciClient(connection, replyQueue.Name, replies);
        }

        public async Task<long> CallAsync(int n, CancellationToken ct)
        {
            string correlationId = Guid.NewGuid().ToString("N");
            var reply = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending[correlationId] = reply;
            await _connection.PublishAsync("", Queue, n.ToString(CultureInfo.InvariantCulture), new PublishOptions
            {
                Properties = new MessageProperties { ReplyTo = _replyQueue, CorrelationId = correlationId },
            }, ct);
            return long.Parse(await reply.Task.WaitAsync(ct), CultureInfo.InvariantCulture);
        }

        private async Task ListenAsync()
        {
            await foreach (var message in _replies)
            {
                if (message.Properties.CorrelationId is { } id && _pending.TryRemove(id, out var reply))
                {
                    reply.TrySetResult(message.GetString());
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _replies.DisposeAsync();
            await _listener;
            await _connection.DisposeAsync();
        }
    }
}
