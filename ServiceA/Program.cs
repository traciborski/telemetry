using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Trace;
using ServiceA;
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
    .AddOpenTelemetry()
    .WithTracing(x => x.AddEntityFrameworkCoreInstrumentation())
    .WithMetrics(x => x.AddMeter(OutboxTelemetry.MeterName, OrdersTelemetry.MeterName));
builder.Services.AddHostedService<OutboxWorker<AppDbContext>>();

builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();
app.UseTenantTelemetry();

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

        var outboxMessage = new TransactionalOutbox
        {
            Topic = KafkaTopics.OrdersCreated,
            Key = message.OrderId.ToString(),
            Payload = JsonSerializer.Serialize(message),
            CreatedAt = message.CreatedAt,
        };
        MessagingTelemetry.InjectTraceContext(outboxMessage.Headers);

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        db.Orders.Add(order);
        db.OutboxMessages.Add(outboxMessage);
        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        OrdersTelemetry.OrdersCreated.Add(
            1,
            new KeyValuePair<string, object?>("product", request.Product),
            new KeyValuePair<string, object?>(MessagingTelemetry.TenantIdAttributeName, TenantTelemetryExtensions.RequireCurrentTenantId()));

        return Results.Accepted(value: new { message.OrderId });
    });
app.MapGet("/health", () => Results.Ok("healthy"));
app.Run();