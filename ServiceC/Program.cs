using Shared.Telemetry;
using ServiceC;
using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Http.Resilience;
using Polly;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceC");
builder.Services.AddSingleton<TenantAccessor>();
var serviceDBaseUrl = builder.Configuration["ServiceD:BaseUrl"] ?? "http://service-d:8080";
builder.Services.AddHttpClient("ServiceD", client => client.BaseAddress = new Uri(serviceDBaseUrl))
    .AddTenantHeaderPropagation()
    .AddResilienceHandler("service-d-retry", resilience =>
    {
        resilience.AddRetry(new HttpRetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            BackoffType = DelayBackoffType.Exponential,
            Delay = TimeSpan.FromMilliseconds(200)
        });
    });

var elasticsearchUri = builder.Configuration["Elasticsearch:Uri"] ?? "http://elasticsearch:9200";
var elasticsearchSettings = new ElasticsearchClientSettings(new Uri(elasticsearchUri)).DefaultIndex("orders-processed");
builder.Services.AddSingleton(new ElasticsearchClient(elasticsearchSettings));

builder.Services.AddOpenTelemetry().WithTracing(tracing => tracing.AddSource("Elastic.Transport"));

builder.Services.AddHostedService<OrderProcessedConsumer>();
builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();
app.UseTenantTelemetry();

app.MapGet("/health", () => Results.Ok("healthy"));
app.Run();
