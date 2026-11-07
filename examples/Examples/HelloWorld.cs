namespace FireMQ.Examples;

/// <summary>Tutorial 1: send one message to a queue and receive it.</summary>
public static class HelloWorld
{
    public static async Task RunAsync(string url, CancellationToken ct)
    {
        await using var client = await FireMQClient.ConnectAsync(url, cancellationToken: ct);
        const string queue = "hello.dotnet";
        await client.DeclareQueueAsync(queue, cancellationToken: ct);

        // Completes when the broker has confirmed the message.
        await client.PublishAsync("", queue, "Hello World!", cancellationToken: ct);
        Console.WriteLine(" [x] Sent 'Hello World!'");

        await using var subscription = await client.SubscribeAsync(queue, cancellationToken: ct);
        await foreach (var message in subscription.WithCancellation(ct))
        {
            Console.WriteLine($" [x] Received '{message.GetString()}'");
            message.Ack();
            break;
        }
    }
}
