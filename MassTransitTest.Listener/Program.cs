using Confluent.Kafka;
using MassTransit;
using MassTransit.EntityFrameworkCoreIntegration;
using MassTransitTest.ApiService.Data;
using MassTransitTest.ApiService.Messages;
using MassTransitTest.ApiService.StateMachines;
using Microsoft.EntityFrameworkCore;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddProblemDetails();
builder.Services.AddOpenApi();
builder.AddNpgsqlDbContext<MessageDbContext>(connectionName: "postgresdb");
builder.Services.AddMassTransit(cfg =>
{
    cfg.UsingInMemory();

    cfg.AddSagaStateMachine<MessageStateMachine, MessageState>()
        .EntityFrameworkRepository(r =>
        {
            r.ConcurrencyMode = ConcurrencyMode.Pessimistic;
            r.ExistingDbContext<MessageDbContext>();
            r.LockStatementProvider = new PostgresLockStatementProvider();
        });

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
            k.Host(builder.Configuration.GetConnectionString("kafka"));

            k.TopicEndpoint<KafkaInbound>(
                "topic-1",
                "group-name",
                e =>
                {
                    e.CreateIfMissing();
                    e.AutoStart = true;
                    e.AutoOffsetReset = AutoOffsetReset.Earliest;
                    e.ConfigureSaga<MessageState>(context);
                });
        });
    });
});

var app = builder.Build();

app.UseExceptionHandler();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

await using (var conn = new NpgsqlConnection(builder.Configuration.GetConnectionString("postgresdb")))
{
    await conn.OpenAsync();
    await using var cmd = conn.CreateCommand();
    cmd.CommandText =
        @"CREATE TABLE IF NOT EXISTS message_states (
            correlationid uuid PRIMARY KEY,
            currentstate integer NOT NULL,
            data text NULL,
            error text NULL,
            version int NOT NULL
            );";
    await cmd.ExecuteNonQueryAsync();
}

app.Run();
