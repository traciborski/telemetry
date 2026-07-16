using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Trace;
using ServiceA.Outbox;
using Shared.Messaging;
using Shared.Messaging.Contracts;
using Shared.Outbox;
using Shared.Telemetry;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceA");
builder.Services.AddSingleton(sp =>
    new KafkaProducer(builder.Configuration["Kafka:BootstrapServers"]));

var sqlConnectionString =
    builder.Configuration["Sql:ConnectionString"] ?? throw new Exception("no sql connection string");
builder.Services
    .AddDbContext<AppDbContext>(x => x.UseSqlServer(sqlConnectionString));
builder.Services
    .AddOpenTelemetry().WithTracing(x =>
        x.AddEntityFrameworkCoreInstrumentation());
builder.Services.AddHostedService<OutboxWorkerBackgroundService<AppDbContext>>();

builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    await scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreatedAsync();
}

app.MapPost("/orders",
    async (OrderRequest request, AppDbContext db, ILogger<Program> logger, CancellationToken cancellationToken) =>
    {
        var message = new OrderCreatedMessage(Guid.NewGuid(), request.Product, request.Quantity, DateTimeOffset.UtcNow);
        logger.LogInformation("Order for {Product} x {Quantity} - {OrderId}", request.Product, request.Quantity, message.OrderId);

        var order = new Order
        {
            Id = message.OrderId,
            Product = request.Product,
            Quantity = request.Quantity,
            CreatedAt = message.CreatedAt,
        };

        var headers = new Headers();
        KafkaTelemetry.InjectTraceContext(Activity.Current, headers);

        var outboxMessage = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            Topic = KafkaTopics.OrdersCreated,
            Key = message.OrderId.ToString(),
            Payload = JsonSerializer.Serialize(message),
            Headers = KafkaTelemetry.SerializeHeaders(headers),
            CreatedAt = message.CreatedAt,
        };

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        db.Orders.Add(order);
        db.OutboxMessages.Add(outboxMessage);
        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        return Results.Accepted(value: new { message.OrderId });
    });
app.MapGet("/health", () => Results.Ok("healthy"));
app.Run();