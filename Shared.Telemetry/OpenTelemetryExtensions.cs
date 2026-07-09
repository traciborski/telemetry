using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Shared.Telemetry;

/// <summary>
/// Single place that wires up OpenTelemetry (logs, traces, metrics) the same way for every service.
/// Everything is shipped via OTLP to the otel-collector, which fans it out to Tempo, Loki and Mimir.
/// </summary>
public static class OpenTelemetryExtensions
{
    /// <summary>
    /// The name of the ActivitySource/Meter used for manual Kafka producer/consumer instrumentation.
    /// Registered for every service so traces stay connected across HTTP + Kafka hops.
    /// </summary>
    public const string KafkaActivitySourceName = "Messaging.Kafka";

    public static WebApplicationBuilder AddServiceTelemetry(this WebApplicationBuilder builder, string serviceName)
    {
        var otlpEndpoint = builder.Configuration["Otel:Endpoint"] ?? "http://otel-collector:4317";
        var otlpUri = new Uri(otlpEndpoint);

        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeFormattedMessage = true;
            logging.IncludeScopes = true;
            logging.ParseStateValues = true;
            logging.AddOtlpExporter(o => o.Endpoint = otlpUri);
        });

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(r => r.AddService(serviceName: serviceName))
            .WithTracing(tracing => tracing
                .AddSource(serviceName)
                .AddSource(KafkaActivitySourceName)
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddOtlpExporter(o => o.Endpoint = otlpUri))
            .WithMetrics(metrics => metrics
                .AddMeter(KafkaActivitySourceName)
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddRuntimeInstrumentation()
                .AddOtlpExporter(o => o.Endpoint = otlpUri));

        return builder;
    }
}
