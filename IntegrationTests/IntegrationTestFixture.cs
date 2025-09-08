using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace IntegrationTests;

public sealed class IntegrationTestFixture : IAsyncLifetime
{
    public PostgreSqlContainer Postgres { get; private set; } = null!;
    public KafkaContainer Kafka { get; private set; } = null!;

    public string ConnectionString => Postgres.GetConnectionString();
    public string BootstrapServers => Kafka.GetBootstrapAddress();

    public const string TopicName = "it-messages";
    public const string GroupId = "it-group";

    public async ValueTask InitializeAsync()
    {
        Postgres = new PostgreSqlBuilder()
            .WithImage("postgres:16")
            .WithPortBinding(5432, 5432)
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();

        await Postgres.StartAsync();

        await using (var conn = new NpgsqlConnection(Postgres.GetConnectionString()))
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

        // Kafka
        Kafka = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.2.15")
            .Build();

        await Kafka.StartAsync();

        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = BootstrapServers,
            SocketKeepaliveEnable = true
        };
        
        var adminClient = new AdminClientBuilder(adminConfig).Build();
        
        var topicSpec = new TopicSpecification
        {
            Name = TopicName,
            NumPartitions = 1
        };

        await adminClient.CreateTopicsAsync([topicSpec]);
    }

    public async ValueTask DisposeAsync()
    {
        await Kafka.DisposeAsync();
        await Postgres.DisposeAsync();
    }
}

[CollectionDefinition("integration")]
public sealed class IntegrationCollection : ICollectionFixture<IntegrationTestFixture> { }