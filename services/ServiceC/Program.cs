using Shared.Telemetry;
using ServiceC;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceTelemetry("ServiceC");

var serviceDBaseUrl = builder.Configuration["ServiceD:BaseUrl"] ?? "http://service-d:8080";
builder.Services.AddHttpClient("ServiceD", client => client.BaseAddress = new Uri(serviceDBaseUrl));
builder.Services.AddHostedService<OrderProcessedConsumer>();

builder.WebHost.UseUrls("http://*:8080");
var app = builder.Build();

app.MapGet("/health", () => Results.Ok("healthy"));

app.Run();
