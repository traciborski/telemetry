using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Shared.Messaging;

/// <summary>
/// Bridges OpenTelemetry's W3C trace-context propagation with Kafka message headers. Kafka has no
/// automatic instrumentation in .NET the way ASP.NET Core / HttpClient do, so producers inject the
/// current Activity into the message headers and consumers extract it back out. That keeps the
/// consumer span (and anything started underneath it, like an outgoing HttpClient call) part of the
/// very same distributed trace as the original HTTP request.
/// </summary>
public static class KafkaTelemetry
{
    public static readonly ActivitySource ActivitySource = new("Messaging.Kafka");

    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    public static void InjectTraceContext(Activity? activity, Headers headers)
    {
        if (activity is null)
        {
            return;
        }

        Propagator.Inject(new PropagationContext(activity.Context, Baggage.Current), headers, InjectHeader);
    }

    public static ActivityContext ExtractTraceContext(Headers headers)
    {
        var propagationContext = Propagator.Extract(default, headers, ExtractHeader);
        Baggage.Current = propagationContext.Baggage;
        return propagationContext.ActivityContext;
    }

    private static void InjectHeader(Headers headers, string key, string value) =>
        headers.Add(key, Encoding.UTF8.GetBytes(value));

    private static IEnumerable<string> ExtractHeader(Headers headers, string key) =>
        headers.TryGetLastBytes(key, out var bytes) ? [Encoding.UTF8.GetString(bytes)] : [];
}
