using Shared.Messaging;
using Shared.Messaging.Contracts;
using Shared.Telemetry;
using ServiceB;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceB");

var bootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "redpanda:9092";
builder.Services.AddSingleton(sp =>
    new KafkaProducer<OrderProcessedMessage>(bootstrapServers, sp.GetRequiredService<ILogger<KafkaProducer<OrderProcessedMessage>>>()));
builder.Services.AddHostedService<OrderCreatedConsumer>();

builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();

app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();
