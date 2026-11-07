namespace FireMQ.Examples;

/// <summary>Tutorial 3: broadcast every message to all subscribers through a fanout exchange.</summary>
public static class PublishSubscribe
{
    public static async Task RunAsync(string url, CancellationToken ct)
    {
        await using var client = await FireMQClient.ConnectAsync(url, cancellationToken: ct);
        const string exchange = "logs.dotnet";
        await client.DeclareExchangeAsync(exchange, ExchangeType.Fanout, cancellationToken: ct);

        // Each subscriber gets its own temporary queue with a broker-generated name.
        var subscribers = new List<(string Name, Subscription Subscription)>();
        foreach (string name in new[] { "A", "B" })
        {
            var queue = await client.DeclareQueueAsync("", QueueOptions.Temporary, ct);
            await client.BindQueueAsync(queue.Name, exchange, "", cancellationToken: ct);
            subscribers.Add((name, await client.SubscribeAsync(queue.Name, new SubscribeOptions { NoAck = true }, ct)));
        }

        string[] logs = { "info: Hello World!", "warning: disk at 80%", "error: disk full" };
        foreach (string log in logs)
        {
            await client.PublishAsync(exchange, "", log, cancellationToken: ct);
            Console.WriteLine($" [x] Sent {log}");
        }

        foreach (var (name, subscription) in subscribers)
        {
            for (int i = 0; i < logs.Length; i++)
            {
                var message = await subscription.NextAsync(ct);
                Console.WriteLine($" [subscriber {name}] {message!.GetString()}");
            }
            await subscription.DisposeAsync();
        }
    }
}
