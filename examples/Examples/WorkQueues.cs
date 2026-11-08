namespace FireMQ.Examples;

/// <summary>Tutorial 2: share time-consuming tasks between workers, one task at a time each.</summary>
public static class WorkQueues
{
    public static async Task RunAsync(string url, CancellationToken ct)
    {
        await using var client = await FireMQClient.ConnectAsync(url, cancellationToken: ct);
        const string queue = "task_queue.dotnet";
        await client.DeclareQueueAsync(queue, cancellationToken: ct);
        await client.PurgeQueueAsync(queue, ct);

        string[] tasks = { "First.", "Second..", "Third...", "Fourth....", "Fifth....." };
        foreach (string task in tasks)
        {
            // Persistent by default, so tasks survive a broker restart.
            await client.PublishAsync("", queue, task, cancellationToken: ct);
            Console.WriteLine($" [x] Sent {task}");
        }

        int remaining = tasks.Length;
        using var done = CancellationTokenSource.CreateLinkedTokenSource(ct);

        async Task Worker(int id)
        {
            // Prefetch 1: a busy worker gets no new task until it acknowledges the current one.
            await using var subscription = await client.SubscribeAsync(queue, new SubscribeOptions { Prefetch = 1 }, ct);
            try
            {
                await foreach (var message in subscription.WithCancellation(done.Token))
                {
                    string body = message.GetString();
                    Console.WriteLine($" [worker {id}] Received {body}");
                    await Task.Delay(body.Count(c => c == '.') * 50, ct);
                    Console.WriteLine($" [worker {id}] Done");
                    message.Ack();
                    if (Interlocked.Decrement(ref remaining) == 0)
                    {
                        done.Cancel();
                    }
                }
            }
            catch (OperationCanceledException) when (done.IsCancellationRequested && !ct.IsCancellationRequested)
            {
            }
        }

        await Task.WhenAll(Worker(1), Worker(2));
    }
}
