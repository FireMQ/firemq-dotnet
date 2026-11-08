namespace FireMQ.Examples;

/// <summary>Tutorial 5: route by pattern with a topic exchange. <c>*</c> matches one word, <c>#</c> zero or more.</summary>
public static class Topics
{
    public static async Task RunAsync(string url, CancellationToken ct)
    {
        await using var client = await FireMQClient.ConnectAsync(url, cancellationToken: ct);
        const string exchange = "topic_logs.dotnet";
        await client.DeclareExchangeAsync(exchange, ExchangeType.Topic, cancellationToken: ct);

        var patterns = new[] { "kern.*", "*.critical", "#" };
        var queues = new Dictionary<string, string>();
        foreach (string pattern in patterns)
        {
            var queue = await client.DeclareQueueAsync("", QueueOptions.Temporary, ct);
            await client.BindQueueAsync(queue.Name, exchange, pattern, cancellationToken: ct);
            queues[pattern] = queue.Name;
        }

        foreach (string key in new[] { "kern.critical", "kern.info", "cron.critical", "auth.info.login" })
        {
            await client.PublishAsync(exchange, key, $"message for {key}", cancellationToken: ct);
            Console.WriteLine($" [x] Sent {key}");
        }

        foreach (var (pattern, queue) in queues)
        {
            var keys = new List<string>();
            while (await client.GetAsync(queue, noAck: true, ct) is { } message)
            {
                keys.Add(message.RoutingKey);
            }
            Console.WriteLine($" [{pattern}] {string.Join(", ", keys)}");
        }
    }
}
