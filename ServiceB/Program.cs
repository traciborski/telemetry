using Shared.Messaging;
using Shared.Messaging.Contracts;
using Shared.Telemetry;
using ServiceB;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceB");
builder.Services.AddSingleton(sp => new KafkaProducer<OrderProcessedMessage>(builder.Configuration["Kafka:BootstrapServers"]));
builder.Services.AddHostedService<OrderCreatedConsumer>();
builder.WebHost.UseUrls("http://*:8080");

var app = builder.Build();
app.MapGet("/health", () => Results.Ok("healthy"));
app.Run();
