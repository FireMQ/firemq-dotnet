using System.Text;

namespace FireMQ.Tests;

/// <summary>A test that needs a running broker. Skipped unless <c>FIREMQ_URL</c> is set.</summary>
public sealed class IntegrationFactAttribute : FactAttribute
{
    public IntegrationFactAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FIREMQ_URL")))
        {
            Skip = "set FIREMQ_URL, for example fire://guest:guest@localhost:5673, to run integration tests";
        }
    }
}

/// <summary>A TLS test. Skipped unless <c>FIREMQ_TLS_URL</c> and <c>FIREMQ_TLS_CA</c> (a PEM file) are set.</summary>
public sealed class TlsFactAttribute : FactAttribute
{
    public TlsFactAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FIREMQ_TLS_URL"))
            || string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FIREMQ_TLS_CA")))
        {
            Skip = "set FIREMQ_TLS_URL and FIREMQ_TLS_CA to run TLS tests";
        }
    }
}

/// <summary>Tests against a real FireMQ broker.</summary>
public class IntegrationTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);
    private static string Url => Environment.GetEnvironmentVariable("FIREMQ_URL")!;
    private static string Name(string what) => $"dotnet.test.{what}.{Guid.NewGuid():N}";

    [IntegrationFact]
    public async Task PublishAndGetRoundTripPropertiesAndHeaders()
    {
        await using var client = await FireMQClient.ConnectAsync(Url, new ConnectOptions { ConnectionName = "dotnet integration tests" });
        Assert.Equal("firemq", client.Server.Name);
        string queue = Name("get");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        Assert.Null(await client.GetAsync(queue, noAck: true));

        await client.PublishAsync("", queue, "hello", new PublishOptions
        {
            Properties = new MessageProperties
            {
                ContentType = "text/plain",
                CorrelationId = "c-1",
                ReplyTo = "replies",
                Expiration = TimeSpan.FromMinutes(1),
                MessageId = "m-1",
                Timestamp = DateTimeOffset.FromUnixTimeSeconds(1_767_225_600),
                Type = "greeting",
                AppId = "tests",
                Priority = 4,
            },
            Headers = new Dictionary<string, object?>
            {
                ["int"] = 42,
                ["float"] = 1.5,
                ["bool"] = true,
                ["nil"] = null,
                ["list"] = new object?[] { 1, "two" },
                ["map"] = new Dictionary<string, object?> { ["k"] = "v" },
            },
        });

        var message = await client.GetAsync(queue, noAck: true);
        Assert.NotNull(message);
        Assert.Equal(("hello", "", queue), (message!.GetString(), message.Exchange, message.RoutingKey));
        var p = message.Properties;
        Assert.Equal(("text/plain", "c-1", "replies", "m-1", "greeting", "tests"), (p.ContentType, p.CorrelationId, p.ReplyTo, p.MessageId, p.Type, p.AppId));
        Assert.Equal(TimeSpan.FromMinutes(1), p.Expiration);
        Assert.Equal(1_767_225_600, p.Timestamp!.Value.ToUnixTimeSeconds());
        Assert.Equal((byte)4, p.Priority);
        Assert.Equal(42L, message.Headers["int"]);
        Assert.Equal(1.5, message.Headers["float"]);
        Assert.Equal(true, message.Headers["bool"]);
        Assert.Null(message.Headers["nil"]);
        Assert.Equal(new object?[] { 1L, "two" }, message.Headers["list"]);
        Assert.Equal("v", ((IDictionary<string, object?>)message.Headers["map"]!)["k"]);
    }

    [IntegrationFact]
    public async Task ConsumeAndAckInOrder()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string queue = Name("consume");
        // Not auto-delete: the queue must outlive the subscription for the count check.
        await client.DeclareQueueAsync(queue, new QueueOptions { Durable = false, Exclusive = true });
        await Task.WhenAll(Enumerable.Range(0, 200).Select(i => client.PublishAsync("", queue, $"m{i}")));

        await using var subscription = await client.SubscribeAsync(queue, new SubscribeOptions { Prefetch = 10 });
        int expected = 0;
        using var timeout = new CancellationTokenSource(Wait);
        await foreach (var message in subscription.WithCancellation(timeout.Token))
        {
            // PublishAsync queues each frame before it returns its task, so loop order holds.
            Assert.Equal($"m{expected}", message.GetString());
            Assert.False(message.Redelivered);
            message.Ack();
            if (++expected == 200)
            {
                break;
            }
        }
        Assert.Equal(200, expected);
        await subscription.CancelAsync();
        var info = await client.DeclareQueueAsync(queue, new QueueOptions { Passive = true });
        Assert.Equal(0ul, info.Messages);
    }

    [IntegrationFact]
    public async Task ManyConcurrentPublishesAreAllConfirmed()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string queue = Name("confirms");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        await Task.WhenAll(Enumerable.Range(0, 2000).Select(i => client.PublishAsync("", queue, BitConverter.GetBytes(i))));
        Assert.Equal(2000ul, await client.PurgeQueueAsync(queue));
    }

    [IntegrationFact]
    public async Task PrefetchLimitsUnacknowledgedDeliveries()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string queue = Name("prefetch");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        for (int i = 0; i < 4; i++)
        {
            await client.PublishAsync("", queue, $"m{i}");
        }
        await using var subscription = await client.SubscribeAsync(queue, new SubscribeOptions { Prefetch = 2 });
        var first = await subscription.NextAsync().AsTask().WaitAsync(Wait);
        var second = await subscription.NextAsync().AsTask().WaitAsync(Wait);
        Assert.Equal(("m0", "m1"), (first!.GetString(), second!.GetString()));
        using (var quiet = new CancellationTokenSource(TimeSpan.FromMilliseconds(300)))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => subscription.NextAsync(quiet.Token).AsTask());
        }
        first.Ack();
        second.Ack();
        var third = await subscription.NextAsync().AsTask().WaitAsync(Wait);
        Assert.Equal("m2", third!.GetString());
    }

    [IntegrationFact]
    public async Task NackRequeuesAndDeadLetters()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string dlx = Name("dlx");
        string dead = Name("dead");
        string work = Name("work");
        await client.DeclareExchangeAsync(dlx, ExchangeType.Fanout, new ExchangeOptions { Durable = false, AutoDelete = true });
        await client.DeclareQueueAsync(dead, QueueOptions.Temporary);
        await client.BindQueueAsync(dead, dlx, "");
        await client.DeclareQueueAsync(work, QueueOptions.Temporary.WithArgument("x-dead-letter-exchange", dlx));

        await client.PublishAsync("", work, "job");
        var once = await client.GetAsync(work);
        once!.Nack(requeue: true);
        var twice = await client.GetAsync(work);
        Assert.True(twice!.Redelivered);
        twice.Nack(requeue: false);

        Message? deadLettered = null;
        for (int i = 0; i < 50 && deadLettered is null; i++)
        {
            deadLettered = await client.GetAsync(dead, noAck: true);
            if (deadLettered is null)
            {
                await Task.Delay(20);
            }
        }
        Assert.NotNull(deadLettered);
        Assert.Equal(("job", dlx), (deadLettered!.GetString(), deadLettered.Exchange));
        Assert.True(deadLettered.Headers.ContainsKey("x-death"));
    }

    [IntegrationFact]
    public async Task MandatoryPublishToNowhereIsUnroutable()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string key = Name("nowhere");
        var e = await Assert.ThrowsAsync<UnroutableException>(() => client.PublishAsync("", key, "lost", new PublishOptions { Mandatory = true }));
        Assert.Equal(("", key, 312), (e.Exchange, e.RoutingKey, e.Code));
        await client.PublishAsync("", key, "dropped");
    }

    [IntegrationFact]
    public async Task FullRejectPublishQueueNacks()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string queue = Name("full");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary.WithArgument("x-max-length", 1).WithArgument("x-overflow", "reject-publish"));
        await client.PublishAsync("", queue, "fits");
        var e = await Assert.ThrowsAsync<NackedException>(() => client.PublishAsync("", queue, "rejected"));
        Assert.Equal(406, e.Code);
    }

    [IntegrationFact]
    public async Task ServerErrorsKeepTheConnectionUsable()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string queue = Name("errors");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        var redeclare = await Assert.ThrowsAsync<ServerException>(() => client.DeclareQueueAsync(queue));
        Assert.Equal(406, redeclare.Code);
        var missing = await Assert.ThrowsAsync<ServerException>(() => client.BindQueueAsync(queue, Name("missing"), ""));
        Assert.Equal(404, missing.Code);
        var passive = await Assert.ThrowsAsync<ServerException>(() => client.DeclareQueueAsync(Name("absent"), new QueueOptions { Passive = true }));
        Assert.Equal(404, passive.Code);
        await client.PublishAsync("", queue, "still works");
        Assert.Equal(1ul, await client.PurgeQueueAsync(queue));
    }

    [IntegrationFact]
    public async Task TopicAndExchangeToExchangeRouting()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string logs = Name("logs");
        string audit = Name("audit");
        string queue = Name("critical");
        await client.DeclareExchangeAsync(logs, ExchangeType.Topic, new ExchangeOptions { Durable = false });
        await client.DeclareExchangeAsync(audit, ExchangeType.Fanout, new ExchangeOptions { Durable = false });
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        await client.BindExchangeAsync(audit, logs, "*.critical.#");
        await client.BindQueueAsync(queue, audit, "");

        foreach (string key in new[] { "kern.critical", "kern.critical.disk", "kern.info", "critical" })
        {
            await client.PublishAsync(logs, key, key);
        }
        var received = new List<string>();
        while (await client.GetAsync(queue, noAck: true) is { } m)
        {
            received.Add(m.GetString());
        }
        Assert.Equal(new[] { "kern.critical", "kern.critical.disk" }, received);

        await client.UnbindExchangeAsync(audit, logs, "*.critical.#");
        await client.PublishAsync(logs, "kern.critical", "after unbind");
        Assert.Null(await client.GetAsync(queue, noAck: true));
        await client.DeleteExchangeAsync(logs);
        await client.DeleteExchangeAsync(audit);
    }

    [IntegrationFact]
    public async Task UnackedMessagesReturnWhenTheConnectionCloses()
    {
        string queue = Name("requeue");
        await using var admin = await FireMQClient.ConnectAsync(Url);
        await admin.DeclareQueueAsync(queue, new QueueOptions { Durable = false });
        try
        {
            await admin.PublishAsync("", queue, "work");
            await using (var worker = await FireMQClient.ConnectAsync(Url))
            {
                var subscription = await worker.SubscribeAsync(queue, new SubscribeOptions { Prefetch = 1 });
                var message = await subscription.NextAsync().AsTask().WaitAsync(Wait);
                Assert.False(message!.Redelivered);
            }
            Message? again = null;
            for (int i = 0; i < 50 && again is null; i++)
            {
                again = await admin.GetAsync(queue, noAck: true);
                if (again is null)
                {
                    await Task.Delay(20);
                }
            }
            Assert.True(again!.Redelivered);
        }
        finally
        {
            await admin.DeleteQueueAsync(queue);
        }
    }

    [IntegrationFact]
    public async Task JsonRoundTrip()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string queue = Name("json");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        await client.PublishJsonAsync("", queue, new Order { Id = 7, Item = "book" });
        var message = await client.GetAsync(queue, noAck: true);
        Assert.Equal("application/json", message!.Properties.ContentType);
        var order = message.Deserialize<Order>();
        Assert.Equal((7, "book"), (order!.Id, order.Item));
    }

    public sealed class Order
    {
        public int Id { get; set; }
        public string Item { get; set; } = "";
    }

    [IntegrationFact]
    public async Task BinaryAndLargeBodiesArriveIntact()
    {
        await using var client = await FireMQClient.ConnectAsync(Url);
        string queue = Name("bodies");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        byte[] binary = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
        byte[] large = Encoding.ASCII.GetBytes(new string('x', 1 << 20));
        await client.PublishAsync("", queue, binary);
        await client.PublishAsync("", queue, large);
        Assert.Equal(binary, (await client.GetAsync(queue, noAck: true))!.Body.ToArray());
        Assert.Equal(large, (await client.GetAsync(queue, noAck: true))!.Body.ToArray());
    }

    [IntegrationFact]
    public async Task WrongPasswordIsRefused()
    {
        var uri = new UriBuilder(Url) { UserName = "guest", Password = "definitely-wrong" };
        var e = await Assert.ThrowsAsync<ServerException>(() => FireMQClient.ConnectAsync(uri.Uri.ToString()));
        Assert.Equal(403, e.Code);
    }

    [TlsFact]
    public async Task ConnectsOverTlsWithACustomCa()
    {
        string url = Environment.GetEnvironmentVariable("FIREMQ_TLS_URL")!;
        string ca = Environment.GetEnvironmentVariable("FIREMQ_TLS_CA")!;

        await Assert.ThrowsAnyAsync<FireMQException>(() => FireMQClient.ConnectAsync(url));

        await using var client = await FireMQClient.ConnectAsync(url, new ConnectOptions { TlsCaPem = ca });
        string queue = Name("tls");
        await client.DeclareQueueAsync(queue, QueueOptions.Temporary);
        await client.PublishAsync("", queue, "secret");
        Assert.Equal("secret", (await client.GetAsync(queue, noAck: true))!.GetString());

        await using var insecure = await FireMQClient.ConnectAsync(url, new ConnectOptions { TlsInsecure = true });
        Assert.False(insecure.IsClosed);
    }
}
