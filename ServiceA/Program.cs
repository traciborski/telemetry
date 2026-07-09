using Shared.Messaging;
using Shared.Messaging.Contracts;
using Shared.Telemetry;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceA");

var bootstrapServers = builder.Configuration["Kafka:BootstrapServers"]!;
builder.Services.AddSingleton(sp => new KafkaProducer<OrderCreatedMessage>(bootstrapServers, sp.GetRequiredService<ILogger<KafkaProducer<OrderCreatedMessage>>>()));

builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();

app.MapPost("/orders", async (OrderRequest request, KafkaProducer<OrderCreatedMessage> producer, ILogger<Program> logger, CancellationToken cancellationToken) =>
{
    var message = new OrderCreatedMessage(Guid.NewGuid(), request.Product, request.Quantity, DateTimeOffset.UtcNow);
    logger.LogInformation("Received order request for {Product} x{Quantity}, assigned OrderId {OrderId}", request.Product, request.Quantity, message.OrderId);
    await producer.PublishAsync(KafkaTopics.OrdersCreated, message.OrderId.ToString(), message, cancellationToken);
    return Results.Accepted(value: new { message.OrderId });
});

app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();
