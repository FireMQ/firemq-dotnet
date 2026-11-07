using System.Text;
using FireMQ.Protocol;

namespace FireMQ.Tests;

/// <summary>
/// Client behaviour against a scripted broker. Tests dispose the fake server before the
/// client, so the client sees the socket close instead of waiting for a close reply.
/// </summary>
public class ClientTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task HandshakeSendsHelloAndExposesWelcome()
    {
        using var broker = new FakeBroker();
        var (client, server, hello) = await broker.ConnectAsync(maxFrame: 4096);
        await using var c = client;
        await using var s = server;

        Assert.Equal(1u, hello.Ver);
        Assert.Equal("/", hello.Vhost);
        Assert.Equal(("plain", "guest", "guest"), (hello.Auth.Mech, hello.Auth.User, hello.Auth.Pass));
        Assert.Equal(0u, hello.Heartbeat);
        Assert.Contains("confirms", hello.Caps);
        Assert.Equal("firemq-dotnet", hello.Client["name"]);
        Assert.Equal("dotnet", hello.Client["lang"]);

        Assert.Equal("fake", client.Server.Name);
        Assert.Equal("test-connection", client.Server.ConnectionId);
        Assert.Equal(4096u, client.Server.MaxFrame);
        Assert.False(client.IsClosed);
    }

    [Fact]
    public async Task ErrorInsteadOfWelcomeIsAServerException()
    {
        using var broker = new FakeBroker();
        var connect = FireMQClient.ConnectAsync(broker.Url.Replace("guest:guest", "guest:wrong"));
        await using var server = await broker.AcceptAsync();
        var hello = await server.ExpectAsync<HelloFrame>();
        Assert.Equal("wrong", hello.Auth.Pass);
        await server.SendAsync(new ErrorFrame { Code = 403, Msg = "ACCESS_REFUSED", Fatal = true });
        var e = await Assert.ThrowsAsync<ServerException>(() => connect);
        Assert.Equal(403, e.Code);
    }

    [Fact]
    public async Task UnreachableBrokerFailsWithFireMQException()
    {
        using var broker = new FakeBroker();
        string url = broker.Url;
        broker.Dispose();
        await Assert.ThrowsAnyAsync<FireMQException>(() => FireMQClient.ConnectAsync(url));
    }

    [Fact]
    public async Task CumulativeConfirmsAndIndividualNacks()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var first = client.PublishAsync("", "q", "1");
        var second = client.PublishAsync("", "q", "2");
        var third = client.PublishAsync("", "q", "3");
        var seqs = new List<ulong>();
        for (int i = 0; i < 3; i++)
        {
            var publish = await server.ExpectAsync<PublishFrame>();
            Assert.True(publish.Persistent);
            seqs.Add(publish.Seq);
        }
        Assert.Equal(new ulong[] { 1, 2, 3 }, seqs);

        await server.SendAsync(new ConfirmFrame { Seq = 2, Nack = true, Code = 406, Msg = "queue 'q' is full" });
        await server.SendAsync(new ConfirmFrame { Upto = 3 });

        await first.WaitAsync(Wait);
        var nacked = await Assert.ThrowsAsync<NackedException>(() => second.WaitAsync(Wait));
        Assert.Equal(406, nacked.Code);
        await third.WaitAsync(Wait);
    }

    [Fact]
    public async Task ReturnBeforeConfirmFailsThePublishAsUnroutable()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var lost = client.PublishAsync("orders", "order.unknown", "hi", new PublishOptions { Mandatory = true });
        var kept = client.PublishAsync("orders", "order.created", "hi", new PublishOptions { Mandatory = true });
        var p1 = await server.ExpectAsync<PublishFrame>();
        var p2 = await server.ExpectAsync<PublishFrame>();
        Assert.True(p1.Mandatory);
        await server.SendAsync(new ReturnFrame { Seq = p1.Seq, Code = 312, Msg = "NO_ROUTE", Exchange = "orders", Key = "order.unknown", Body = Encoding.UTF8.GetBytes("hi") });
        await server.SendAsync(new ConfirmFrame { Upto = p2.Seq });

        var e = await Assert.ThrowsAsync<UnroutableException>(() => lost.WaitAsync(Wait));
        Assert.Equal(("orders", "order.unknown", 312), (e.Exchange, e.RoutingKey, e.Code));
        await kept.WaitAsync(Wait);
    }

    [Fact]
    public async Task PublishWithoutConfirmSendsSequenceZero()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        await client.PublishAsync("", "q", "fire and forget", new PublishOptions { Confirm = false, Persistent = false }).WaitAsync(Wait);
        var publish = await server.ExpectAsync<PublishFrame>();
        Assert.Equal((0ul, false), (publish.Seq, publish.Persistent));

        var confirmed = client.PublishAsync("", "q", "confirmed");
        Assert.Equal(1ul, (await server.ExpectAsync<PublishFrame>()).Seq);
        await server.SendAsync(new ConfirmFrame { Upto = 1 });
        await confirmed.WaitAsync(Wait);
    }

    [Fact]
    public async Task OversizedPublishFailsWithoutUsingASequenceNumber()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync(maxFrame: 1024);
        await using var c = client;
        await using var s = server;

        await Assert.ThrowsAsync<ArgumentException>(() => client.PublishAsync("", "q", new byte[2048]));
        var ok = client.PublishAsync("", "q", new byte[100]);
        Assert.Equal(1ul, (await server.ExpectAsync<PublishFrame>()).Seq);
        await server.SendAsync(new ConfirmFrame { Upto = 1 });
        await ok.WaitAsync(Wait);
    }

    [Fact]
    public async Task PublishJsonSetsContentType()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var task = client.PublishJsonAsync("", "q", new { Id = 42 });
        var publish = await server.ExpectAsync<PublishFrame>();
        Assert.Equal("application/json", publish.Props!.ContentType);
        Assert.Equal("{\"Id\":42}", Encoding.UTF8.GetString(publish.Body.Span));
        await server.SendAsync(new ConfirmFrame { Upto = publish.Seq });
        await task.WaitAsync(Wait);
    }

    [Fact]
    public async Task ErrorWithRequestIdFailsOnlyThatRequest()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var bad = client.DeclareQueueAsync("jobs", new QueueOptions { Durable = false });
        var declare = await server.ExpectAsync<DeclareQueueFrame>();
        Assert.Equal(("jobs", false), (declare.Name, declare.Durable));
        await server.SendAsync(new ErrorFrame { Rid = declare.Rid, Code = 406, Msg = "PRECONDITION_FAILED - inequivalent arg 'durable'" });
        var e = await Assert.ThrowsAsync<ServerException>(() => bad.WaitAsync(Wait));
        Assert.Equal(406, e.Code);

        var good = client.DeclareQueueAsync("");
        var again = await server.ExpectAsync<DeclareQueueFrame>();
        Assert.True(again.Durable);
        await server.SendAsync(new OkFrame { Rid = again.Rid, QueueName = "amq.gen-1", Messages = 3, Consumers = 1 });
        var info = await good.WaitAsync(Wait);
        Assert.Equal(("amq.gen-1", 3ul, 1ul), (info.Name, info.Messages, info.Consumers));
    }

    [Fact]
    public async Task CreditIsToppedUpInHalfWindowBatchesOnAck()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var subscribing = client.SubscribeAsync("jobs", new SubscribeOptions { Prefetch = 4 });
        var subscribe = await server.ExpectAsync<SubscribeFrame>();
        Assert.Equal(("jobs", 4u, false), (subscribe.Queue, subscribe.Credit, subscribe.NoAck));
        await server.SendAsync(new OkFrame { Rid = subscribe.Rid });
        var subscription = await subscribing.WaitAsync(Wait);

        for (ulong tag = 1; tag <= 4; tag++)
        {
            await server.SendAsync(new DeliverFrame { Sub = subscribe.Sub, Tag = tag, Key = "jobs", Body = Encoding.UTF8.GetBytes($"m{tag}") });
        }
        for (int i = 1; i <= 4; i++)
        {
            var message = await subscription.NextAsync().AsTask().WaitAsync(Wait);
            Assert.Equal($"m{i}", message!.GetString());
            message.Ack();
            message.Ack(); // settling twice is a no-op
        }

        var frames = new List<string>();
        for (int i = 0; i < 6; i++)
        {
            frames.Add((await server.ReadAsync()) switch
            {
                AckFrame a => $"ack {a.Tag}",
                CreditFrame cr => $"credit {cr.Credit}",
                var other => other.OpName,
            });
        }
        Assert.Equal(new[] { "ack 1", "ack 2", "credit 2", "ack 3", "ack 4", "credit 2" }, frames);
    }

    [Fact]
    public async Task NoAckCreditIsGrantedAsMessagesAreRead()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var subscribing = client.SubscribeAsync("jobs", new SubscribeOptions { Prefetch = 2, NoAck = true });
        var subscribe = await server.ExpectAsync<SubscribeFrame>();
        Assert.True(subscribe.NoAck);
        await server.SendAsync(new OkFrame { Rid = subscribe.Rid });
        var subscription = await subscribing.WaitAsync(Wait);

        await server.SendAsync(new DeliverFrame { Sub = subscribe.Sub, Tag = 1, Body = new byte[] { 1 } });
        Assert.True(await server.IsQuietAsync(TimeSpan.FromMilliseconds(200)));
        var message = await subscription.NextAsync().AsTask().WaitAsync(Wait);
        Assert.True(message!.IsSettled);
        message.Ack(); // no-op for no-ack messages
        var credit = Assert.IsType<CreditFrame>(await server.NextAsync());
        Assert.Equal((subscribe.Sub, 1u), (credit.Sub, credit.Credit));
    }

    [Fact]
    public async Task NackCarriesRequeue()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var getting = client.GetAsync("jobs");
        var get = await server.ExpectAsync<GetFrame>();
        Assert.False(get.NoAck);
        await server.SendAsync(new GetOkFrame
        {
            Rid = get.Rid,
            Tag = 9,
            Redelivered = true,
            Exchange = "ex",
            Key = "jobs",
            Remaining = 2,
            Props = new MessageProperties { CorrelationId = "c-1" },
            Headers = new Dictionary<string, object?> { ["n"] = 5 },
            Body = Encoding.UTF8.GetBytes("{\"id\":7}"),
        });
        var message = await getting.WaitAsync(Wait);
        Assert.NotNull(message);
        Assert.Equal((9ul, true, "ex", "jobs", "c-1"), (message!.DeliveryTag, message.Redelivered, message.Exchange, message.RoutingKey, message.Properties.CorrelationId));
        Assert.Equal(5L, message.Headers["n"]);
        Assert.Equal(7, message.Deserialize<Payload>()!.Id);
        message.Nack(requeue: true);
        var nack = await server.ExpectAsync<NackFrame>();
        Assert.Equal((9ul, true, false), (nack.Tag, nack.Requeue, nack.Multiple));

        var empty = client.GetAsync("jobs", noAck: true);
        var get2 = await server.ExpectAsync<GetFrame>();
        Assert.True(get2.NoAck);
        await server.SendAsync(new OkFrame { Rid = get2.Rid, Empty = true });
        Assert.Null(await empty.WaitAsync(Wait));
    }

    public sealed class Payload
    {
        [System.Text.Json.Serialization.JsonPropertyName("id")]
        public int Id { get; set; }
    }

    [Fact]
    public async Task CancelledSubscriptionEndsTheEnumeration()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var subscribing = client.SubscribeAsync("jobs");
        var subscribe = await server.ExpectAsync<SubscribeFrame>();
        await server.SendAsync(new OkFrame { Rid = subscribe.Rid });
        var subscription = await subscribing.WaitAsync(Wait);
        await server.SendAsync(new DeliverFrame { Sub = subscribe.Sub, Tag = 1, Body = new byte[] { 1 } });
        await server.SendAsync(new CancelledFrame { Sub = subscribe.Sub, Code = 404, Msg = "queue 'jobs' was deleted" });

        var received = new List<ulong>();
        await foreach (var message in subscription.WithCancellation(new CancellationTokenSource(Wait).Token))
        {
            received.Add(message.DeliveryTag);
        }
        Assert.Equal(new ulong[] { 1 }, received);
        Assert.Equal(404, subscription.CancelCode);
    }

    [Fact]
    public async Task CancelAsyncUnsubscribesAndWaitsForTheBroker()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var subscribing = client.SubscribeAsync("jobs", new SubscribeOptions { Tag = "worker-1", Exclusive = true });
        var subscribe = await server.ExpectAsync<SubscribeFrame>();
        Assert.Equal(("worker-1", true), (subscribe.Tag, subscribe.Exclusive));
        await server.SendAsync(new OkFrame { Rid = subscribe.Rid });
        var subscription = await subscribing.WaitAsync(Wait);

        var cancel = subscription.CancelAsync();
        var unsubscribe = await server.ExpectAsync<UnsubscribeFrame>();
        Assert.Equal(subscribe.Sub, unsubscribe.Sub);
        Assert.False(cancel.IsCompleted);
        await server.SendAsync(new OkFrame { Rid = unsubscribe.Rid });
        await cancel.WaitAsync(Wait);
        Assert.Null(await subscription.NextAsync().AsTask().WaitAsync(Wait));
    }

    [Fact]
    public async Task PingIsAnsweredWithPong()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        await server.SendAsync(PingFrame.Instance);
        await server.ExpectAsync<PongFrame>();
    }

    [Fact]
    public async Task IdleClientSendsHeartbeats()
    {
        using var broker = new FakeBroker();
        var (client, server, hello) = await broker.ConnectAsync(heartbeat: 2, options: new ConnectOptions { Heartbeat = TimeSpan.FromSeconds(2) });
        await using var c = client;
        await using var s = server;

        Assert.Equal(2u, hello.Heartbeat);
        Assert.Equal(TimeSpan.FromSeconds(2), client.Server.Heartbeat);
        await server.ExpectAsync<PingFrame>(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task MissedHeartbeatsCloseTheConnection()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync(heartbeat: 1, options: new ConnectOptions { Heartbeat = TimeSpan.FromSeconds(1) });
        await using var c = client;
        await using var s = server;

        await client.Completion.WaitAsync(TimeSpan.FromSeconds(6));
        Assert.True(client.IsClosed);
        Assert.Contains("heartbeat", client.CloseReason!.Message);
        await Assert.ThrowsAsync<ConnectionClosedException>(() => client.DeclareQueueAsync("x"));
    }

    [Fact]
    public async Task BrokerCloseFailsPendingWorkAndIsAnswered()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var subscribing = client.SubscribeAsync("jobs");
        var subscribe = await server.ExpectAsync<SubscribeFrame>();
        await server.SendAsync(new OkFrame { Rid = subscribe.Rid });
        var subscription = await subscribing.WaitAsync(Wait);

        var pending = client.PurgeQueueAsync("jobs");
        await server.ExpectAsync<PurgeQueueFrame>();
        await server.SendAsync(new CloseFrame { Code = 320, Msg = "CONNECTION_FORCED - shutting down" });

        var e = await Assert.ThrowsAsync<ConnectionClosedException>(() => pending.WaitAsync(Wait));
        Assert.Equal(320, e.Code);
        var reply = await server.ExpectAsync<CloseFrame>();
        Assert.Equal(200, reply.Code);
        await Assert.ThrowsAsync<ConnectionClosedException>(() => subscription.NextAsync().AsTask().WaitAsync(Wait));
    }

    [Fact]
    public async Task FatalErrorClosesTheConnection()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        var pending = client.DeleteExchangeAsync("orders");
        var delete = await server.ExpectAsync<DeleteExchangeFrame>();
        await server.SendAsync(new ErrorFrame { Rid = delete.Rid, Code = 501, Msg = "FRAME_ERROR", Fatal = true });
        var e = await Assert.ThrowsAsync<ServerException>(() => pending.WaitAsync(Wait));
        Assert.Equal(501, e.Code);
        await client.Completion.WaitAsync(Wait);
        Assert.Equal(501, client.CloseReason!.Code);
    }

    [Fact]
    public async Task LostConnectionFailsSubscriptionsAndRequests()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;

        var subscribing = client.SubscribeAsync("jobs");
        var subscribe = await server.ExpectAsync<SubscribeFrame>();
        await server.SendAsync(new OkFrame { Rid = subscribe.Rid });
        var subscription = await subscribing.WaitAsync(Wait);
        var confirm = client.PublishAsync("", "jobs", "x");
        await server.ExpectAsync<PublishFrame>();

        await server.DisposeAsync();

        await Assert.ThrowsAsync<ConnectionClosedException>(() => confirm.WaitAsync(Wait));
        await Assert.ThrowsAsync<ConnectionClosedException>(() => subscription.NextAsync().AsTask().WaitAsync(Wait));
        Assert.Throws<ConnectionClosedException>(() => client.Send(PingFrame.Instance));
    }

    [Fact]
    public async Task CloseAsyncSendsCloseAndEndsSubscriptionsQuietly()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var s = server;

        var subscribing = client.SubscribeAsync("jobs");
        var subscribe = await server.ExpectAsync<SubscribeFrame>();
        await server.SendAsync(new OkFrame { Rid = subscribe.Rid });
        var subscription = await subscribing.WaitAsync(Wait);

        var closing = client.CloseAsync();
        var close = await server.ExpectAsync<CloseFrame>();
        Assert.Equal(200, close.Code);
        await server.SendAsync(new CloseFrame { Code = 200 });
        await closing.WaitAsync(Wait);

        Assert.True(client.IsClosed);
        Assert.Null(await subscription.NextAsync().AsTask().WaitAsync(Wait));
        await Assert.ThrowsAsync<ConnectionClosedException>(() => client.PublishAsync("", "q", "late"));
        await client.DisposeAsync();
    }

    [Fact]
    public async Task FlowPausesPublishersUntilResumed()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        await server.SendAsync(new FlowFrame { Pause = true });
        await server.SendAsync(PingFrame.Instance);
        await server.ExpectAsync<PongFrame>(); // the flow frame has been handled

        var publish = client.PublishAsync("", "q", "held");
        Assert.True(await server.IsQuietAsync(TimeSpan.FromMilliseconds(300)));
        await server.SendAsync(new FlowFrame { Pause = false });
        var frame = Assert.IsType<PublishFrame>(await server.NextAsync());
        await server.SendAsync(new ConfirmFrame { Upto = frame.Seq });
        await publish.WaitAsync(Wait);
    }

    [Fact]
    public async Task CancellingAPublishStopsWaitingForItsConfirm()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        using var cts = new CancellationTokenSource();
        var publish = client.PublishAsync("", "q", "slow", cancellationToken: cts.Token);
        await server.ExpectAsync<PublishFrame>();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publish.WaitAsync(Wait));
        await server.SendAsync(new ConfirmFrame { Upto = 1 });

        var next = client.PublishAsync("", "q", "next");
        Assert.Equal(2ul, (await server.ExpectAsync<PublishFrame>()).Seq);
        await server.SendAsync(new ConfirmFrame { Upto = 2 });
        await next.WaitAsync(Wait);
    }

    [Fact]
    public async Task TopologyRequestsCarryTheirFields()
    {
        using var broker = new FakeBroker();
        var (client, server, _) = await broker.ConnectAsync();
        await using var c = client;
        await using var s = server;

        async Task<T> Answer<T>(Task call, ulong? messages = null) where T : Frame
        {
            var frame = await server.ExpectAsync<T>();
            uint rid = frame switch
            {
                DeclareExchangeFrame f => f.Rid,
                BindFrame f => f.Rid,
                DeleteQueueFrame f => f.Rid,
                _ => throw new InvalidOperationException(),
            };
            await server.SendAsync(new OkFrame { Rid = rid, Messages = messages });
            await call.WaitAsync(Wait);
            return frame;
        }

        var exchange = await Answer<DeclareExchangeFrame>(client.DeclareExchangeAsync("logs", ExchangeType.Topic, new ExchangeOptions().WithArgument("alternate-exchange", "ae")));
        Assert.Equal(("logs", "topic", true), (exchange.Name, exchange.Type, exchange.Durable));
        Assert.Equal("ae", exchange.Args!["alternate-exchange"]);

        var bind = await Answer<BindFrame>(client.BindQueueAsync("q", "logs", "*.error"));
        Assert.Equal((false, "q", "logs", "*.error", ""), (bind.Unbind, bind.Queue, bind.Exchange, bind.Key, bind.ToExchange));

        var unbind = await Answer<BindFrame>(client.UnbindQueueAsync("q", "logs", "*.error"));
        Assert.True(unbind.Unbind);

        var e2e = await Answer<BindFrame>(client.BindExchangeAsync("audit", "logs", "#"));
        Assert.Equal(("audit", "logs", "", "#"), (e2e.ToExchange, e2e.Exchange, e2e.Queue, e2e.Key));

        var deleting = client.DeleteQueueAsync("q", ifEmpty: true);
        var delete = await server.ExpectAsync<DeleteQueueFrame>();
        Assert.Equal(("q", true, false), (delete.Name, delete.IfEmpty, delete.IfUnused));
        await server.SendAsync(new OkFrame { Rid = delete.Rid, Messages = 4 });
        Assert.Equal(4ul, await deleting.WaitAsync(Wait));
    }
}
