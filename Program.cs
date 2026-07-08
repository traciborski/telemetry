using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using Microsoft.AspNetCore.Hosting;
using OpenTelemetry.Exporter;
using System.IO;

var builder = WebApplication.CreateBuilder();
// builder.Logging.ClearProviders();

Directory.CreateDirectory("logs");
var fileStream = new StreamWriter("logs/logs.txt");
Console.SetOut(fileStream);

builder.Services
    .AddOpenTelemetry()
        .WithLogging(
            x => x
                .AddOtlpExporter(
                    y => y.Endpoint = new("http://localhost:4317")
                )
                .AddConsoleExporter(y =>
                    y.Targets = ConsoleExporterOutputTargets.Console
                )
        )
        .WithTracing(x => x
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            //.AddEntityFrameworkCoreInstrumentation() // If using EF Core
            .AddOtlpExporter(y => y.Endpoint = new Uri("http://localhost:4317")))
        .WithMetrics(x => x
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(y => y.Endpoint = new Uri("http://localhost:4317")));

// builder.Services.AddHttpClient<ExternalApiClient>(client =>
// {
//     client.BaseAddress = new Uri("https://api.external.com");
// })
// .AddHttpMessageHandler<CorrelationIdHandler>();

builder.WebHost.UseUrls("http://*:5000");
var app = builder.Build();

app.MapGet("/", (ILogger<Program> logger) =>
{
    logger.LogInformation("Food changed to {price}", 9.99);
    return "Hello from OpenTelemetry Logs!";
});

app.MapGet("/loop", async (ILogger<Program> logger) =>
{
    logger.LogCritical("critical {price}", 9.99);
    var client = new HttpClient
    {
        BaseAddress = new Uri("http://localhost:5001")
    };
    var response = await client.GetAsync("/loop2");
    return await response.Content.ReadAsStringAsync();
});

app.MapGet("/loop2", async (ILogger<Program> logger) =>
{
    logger.LogInformation("Food changed to {price}", 9.98);
    return "loop end";
});

// Add request logging middleware
// app.UseSerilogRequestLogging(options =>
// {
//     options.MessageTemplate = "HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0000} ms";
//     options.EnrichDiagnosticContext = (diagnosticContext, httpContext) =>
//     {
//         diagnosticContext.Set("RequestHost", httpContext.Request.Host.Value);
//         diagnosticContext.Set("RequestScheme", httpContext.Request.Scheme);
//         diagnosticContext.Set("UserAgent", httpContext.Request.Headers["User-Agent"].FirstOrDefault());
//         // Add custom business context
//         if (httpContext.User.Identity.IsAuthenticated)
//         {
//             diagnosticContext.Set("UserId", httpContext.User.FindFirst("sub")?.Value);
//         }
//     };
// });

app.Run();