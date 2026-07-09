using Shared.Messaging.Contracts;
using Shared.Telemetry;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Trace;
using ServiceD;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceD");

var sqlConnectionString = builder.Configuration["Sql:ConnectionString"] ?? throw new Exception("no sql connection string");
builder.Services.AddDbContext<AppDbContext>(options => options.UseSqlServer(sqlConnectionString));
builder.Services.AddOpenTelemetry().WithTracing(tracing => tracing.AddEntityFrameworkCoreInstrumentation());

builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();

app.MapPost("/notifications", async (OrderProcessedMessage message, AppDbContext db, ILogger<Program> logger) =>
{
    logger.LogInformation("Notification received for order {OrderId} ({Product} x{Quantity})", message.OrderId, message.Product, message.Quantity);
    var serverTime = await db.Database.SqlQueryRaw<DateTime>("SELECT GETDATE() AS Value").SingleAsync();
    logger.LogInformation("SQL Server time is {ServerTime}", serverTime);

    return Results.Ok(new { Status = "Delivered", message.OrderId, ReceivedAt = DateTimeOffset.UtcNow, SqlServerTime = serverTime });
});
app.MapGet("/health", () => Results.Ok("healthy"));
app.Run();
