using System.Security.Cryptography;
using System.Text.Json;
using Confluent.Kafka;
using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;
using MassTransit.Testing;
using MassTransitTest.ApiService.Data;
using MassTransitTest.ApiService.Messages;
using MassTransitTest.ApiService.StateMachines;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace IntegrationTests;

[Collection("integration")]
public sealed class StateMachineKafkaIntegrationTests
{
    private readonly IntegrationTestFixture _fx;
    public StateMachineKafkaIntegrationTests(IntegrationTestFixture fx) => _fx = fx;

    private ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();

        services.AddDbContext<MessageDbContext>(opt => opt.UseNpgsql(_fx.ConnectionString));
        services.AddSingleton<ILoggerProvider>(_ => new XUnitLoggerProvider(TestContext.Current.TestOutputHelper!));
        services.AddLogging();
        services.AddMassTransitTestHarness(cfg =>
        {
            cfg.UsingInMemory((context, bus) => bus.ConfigureEndpoints(context));

            cfg.AddRider(rider =>
            {
                rider.AddSagaStateMachine<MessageStateMachine, MessageState>()
                    .EntityFrameworkRepository(r =>
                    {
                        r.ConcurrencyMode = ConcurrencyMode.Pessimistic;
                        r.ExistingDbContext<MessageDbContext>();
                        r.LockStatementProvider = new PostgresLockStatementProvider();
                    });
                
                rider.UsingKafka((context, k) =>
                {
                    k.Host(_fx.BootstrapServers);

                    k.TopicEndpoint<KafkaInbound>(
                        IntegrationTestFixture.TopicName,
                        IntegrationTestFixture.GroupId,
                        e =>
                        {
                            e.AutoStart = true;
                            e.AutoOffsetReset = AutoOffsetReset.Earliest;
                            e.ConfigureSaga<MessageState>(context);
                        });
                });
            });
        });

        return services.BuildServiceProvider(validateScopes: true);
    }

    private char[] _chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

    private string GenerateId()
    {
        var data = new byte[64];
        using var crypto = RandomNumberGenerator.Create();
        crypto.GetBytes(data);
        var result = new char[64];
        for (var i = 0; i < result.Length; i++)
        {
            var idx = data[i] % _chars.Length;
            result[i] = _chars[idx];
        }
        return new string(result);
    }

    [Fact]
    public async Task Success_path_should_finalize_and_delete_saga()
    {
        await using var provider = BuildProvider();
        var harness = provider.GetRequiredService<ITestHarness>();
        await harness.Start();

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _fx.BootstrapServers,
        };
        var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var id = GenerateId();

        await producer.ProduceAsync("it-messages",
            new Message<string, string>
                { Value = JsonSerializer.Serialize(new KafkaInbound { SomeId = id, Data = "ok" }) },
            TestContext.Current.CancellationToken);

        await harness.Consumed.Any<KafkaInbound>(TestContext.Current.CancellationToken);
        
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<MessageDbContext>();
        var saga = await db.MessageStates.FirstOrDefaultAsync(x => x.CorrelationId.Equals(id),
            TestContext.Current.CancellationToken);
        Assert.Null(saga);
    }

    [Fact]
    public async Task Failure_path_should_persist_failed_saga()
    {
        var provider = BuildProvider();
        var harness = provider.GetRequiredService<ITestHarness>();
        await harness.Start();

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _fx.BootstrapServers,
        };
        var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var id = GenerateId();

        await producer.ProduceAsync("it-messages",
            new Message<string, string>
                { Value = JsonSerializer.Serialize(new KafkaInbound { SomeId = id, Data = "" }) },
            TestContext.Current.CancellationToken);
        
        await harness.Consumed.Any<KafkaInbound>(TestContext.Current.CancellationToken);

        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<MessageDbContext>();
        var saga = await db.MessageStates.FirstOrDefaultAsync(cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(saga);
        Assert.False(string.IsNullOrEmpty(saga!.Error));
    }
}