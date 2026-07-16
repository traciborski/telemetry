using Shared.Messaging;
using Shared.Messaging.Contracts;
using Shared.Telemetry;
using StackExchange.Redis;
using OpenTelemetry.Trace;
using ServiceB;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceB");
builder.Services.AddSingleton(sp => new KafkaProducer(builder.Configuration["Kafka:BootstrapServers"]));

var redisConnection = ConnectionMultiplexer.Connect(builder.Configuration["Redis:ConnectionString"] ?? throw new Exception("no redis"));
builder.Services.AddSingleton<IConnectionMultiplexer>(redisConnection);
builder.Services.AddOpenTelemetry().WithTracing(tracing => tracing.AddRedisInstrumentation(redisConnection));

builder.Services.AddHostedService<OrderCreatedConsumer>();
builder.WebHost.UseUrls("http://*:8080");

var app = builder.Build();
app.MapGet("/health", () => Results.Ok("healthy"));
app.Run();
