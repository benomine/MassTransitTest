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
using Npgsql;
using Dapper;

namespace IntegrationTests;

[Collection("integration")]
public sealed class StateMachineKafkaIntegrationTests
{
    private readonly IntegrationTestFixture _fx;
    public StateMachineKafkaIntegrationTests(IntegrationTestFixture fx) => _fx = fx;

    private ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();

        services.AddNpgsqlDataSource(_fx.ConnectionString, builder =>
        {
            builder.EnableParameterLogging();
            builder.EnableDynamicJson();
        });
        services.AddDbContext<MessageDbContext>((a, b) =>
        {
            var source = a.GetRequiredService<NpgsqlDataSource>();
            b.UseNpgsql(source);
            b.EnableDetailedErrors();
        });
        services.AddSingleton<ILoggerProvider>(_ => new XUnitLoggerProvider(TestContext.Current.TestOutputHelper!));
        services.AddLogging();
        services.AddMassTransitTestHarness(cfg =>
        {
            cfg.AddSagaStateMachine<MessageStateMachine, MessageState>()
                .EntityFrameworkRepository(r =>
                {
                    r.ConcurrencyMode = ConcurrencyMode.Pessimistic;
                    r.ExistingDbContext<MessageDbContext>();
                    r.LockStatementProvider = new PostgresLockStatementProvider();
                });
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
        
        var db = provider.GetRequiredService<NpgsqlDataSource>();
        var saga = await (await db.OpenConnectionAsync(TestContext.Current.CancellationToken))
            .QueryAsync<MessageState>("select * from message_states");
        Assert.NotNull(saga);
        Assert.False(saga.Any());
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
        
        var db = provider.GetRequiredService<NpgsqlDataSource>();
        var connection = await db.OpenConnectionAsync(TestContext.Current.CancellationToken);
        var saga = await connection
            .QueryAsync<MessageState>("select * from message_states");
        
        Assert.Single(saga);
        
        var first = saga.Single();
        Assert.Equal("", first.Data);
        Assert.Equal("Données invalides", first.Error);
    }
}