using Shared.Messaging.Contracts;
using Shared.Telemetry;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceD");

builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();

app.MapPost("/notifications", (OrderProcessedMessage message, ILogger<Program> logger) =>
{
    logger.LogInformation(
        "Notification received for order {OrderId} ({Product} x{Quantity})",
        message.OrderId, message.Product, message.Quantity);

    return Results.Ok(new { Status = "Delivered", message.OrderId, ReceivedAt = DateTimeOffset.UtcNow });
});

app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();
