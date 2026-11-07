namespace FireMQ.Examples;

/// <summary>Tutorial 4: route messages by severity with a direct exchange.</summary>
public static class Routing
{
    public static async Task RunAsync(string url, CancellationToken ct)
    {
        await using var client = await FireMQClient.ConnectAsync(url, cancellationToken: ct);
        const string exchange = "direct_logs.dotnet";
        await client.DeclareExchangeAsync(exchange, ExchangeType.Direct, cancellationToken: ct);

        // One queue keeps only errors (say, for disk); the other prints everything.
        var bindings = new Dictionary<string, string[]>
        {
            ["errors"] = new[] { "error" },
            ["console"] = new[] { "info", "warning", "error" },
        };
        var queues = new Dictionary<string, string>();
        foreach (var (name, severities) in bindings)
        {
            var queue = await client.DeclareQueueAsync("", QueueOptions.Temporary, ct);
            foreach (string severity in severities)
            {
                await client.BindQueueAsync(queue.Name, exchange, severity, cancellationToken: ct);
            }
            queues[name] = queue.Name;
        }

        foreach (var (severity, text) in new[] { ("info", "user logged in"), ("warning", "slow request"), ("error", "payment failed") })
        {
            await client.PublishAsync(exchange, severity, text, cancellationToken: ct);
            Console.WriteLine($" [x] Sent {severity}: {text}");
        }

        foreach (var (name, queue) in queues)
        {
            while (await client.GetAsync(queue, noAck: true, ct) is { } message)
            {
                Console.WriteLine($" [{name}] {message.RoutingKey}: {message.GetString()}");
            }
        }
    }
}
