using Aspire.Hosting;
using Npgsql;

var builder = DistributedApplication.CreateBuilder(args);

var username = builder.AddParameter("username", secret: true);
var password = builder.AddParameter("password", secret: true);

var kafka = builder
    .AddKafka("kafka")
    .WithKafkaUI()
    ;

var postgres = builder.AddPostgres("postgres", username, password).WithImage("postgres:16").WithPgWeb();
var postgresdb = postgres.AddDatabase("postgresdb");

builder.AddProject<Projects.MassTransitTest_Listener>("listener")
    .WithReference(kafka)
    .WaitFor(kafka)
    .WithReference(postgresdb)
    .WaitFor(postgresdb);

builder.Build().Run();
