using System.Text.Json;
using Confluent.Kafka;
using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;
using MassTransit.Testing;
using MassTransitTest.ApiService.Consumers;
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
        services.AddSingleton<ILoggerProvider>(_ => new XUnitLoggerProvider(TestContext.Current.TestOutputHelper));
        services.AddLogging();
        services.AddMassTransitTestHarness(cfg =>
        {
            cfg.AddConsumer<MessageProcessingConsumer>();
            cfg.AddConsumer<InitialConsumer>();
            cfg.UsingInMemory((context, bus) => bus.ConfigureEndpoints(context));

            cfg.AddSagaStateMachine<MessageStateMachine, MessageState>()
                .EntityFrameworkRepository(r =>
                {
                    r.ConcurrencyMode = ConcurrencyMode.Pessimistic;
                    r.ExistingDbContext<MessageDbContext>();
                    r.LockStatementProvider = new PostgresLockStatementProvider();
                });
            cfg.AddRider(rider =>
            {
                rider.AddConsumer<InitialConsumer>();

                rider.UsingKafka((context, k) =>
                {
                    k.Host(_fx.BootstrapServers);

                    k.TopicEndpoint<KafkaInbound>(
                        IntegrationTestFixture.TopicName,
                        IntegrationTestFixture.GroupId,
                        e => { e.ConfigureConsumer<InitialConsumer>(context); });
                });
            });
        });

        return services.BuildServiceProvider(validateScopes: true);
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

        var correlationId = Guid.NewGuid();

        await producer.ProduceAsync("it-messages",
            new Message<string, string>
                { Value = JsonSerializer.Serialize(new KafkaInbound { CorrelationId = correlationId, Data = "ok" }) },
            TestContext.Current.CancellationToken);

        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<MessageDbContext>();
        var saga = await db.MessageStates.FirstOrDefaultAsync(x => x.CorrelationId.Equals(correlationId),
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

        var correlationId = Guid.NewGuid();

        await producer.ProduceAsync("it-messages",
            new Message<string, string>
                { Value = JsonSerializer.Serialize(new KafkaInbound { CorrelationId = correlationId, Data = "ok" }) },
            TestContext.Current.CancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        var sagaHarness = harness.GetSagaStateMachineHarness<MessageStateMachine, MessageState>();

        Assert.True(await sagaHarness.Consumed.Any<MessageReceived>(TestContext.Current.CancellationToken)); 
        
        await using var scope = provider.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<MessageDbContext>();
        var saga = await db.MessageStates.FirstOrDefaultAsync(cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(saga);
        Assert.False(string.IsNullOrEmpty(saga!.Error));
    }
}