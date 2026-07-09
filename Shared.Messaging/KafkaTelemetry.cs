using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Shared.Messaging;

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

    private static void InjectHeader(Headers headers, string key, string value)
        => headers.Add(key, Encoding.UTF8.GetBytes(value));

    private static IEnumerable<string> ExtractHeader(Headers headers, string key)
        => headers.TryGetLastBytes(key, out var bytes) ? [Encoding.UTF8.GetString(bytes)] : [];
}
