using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry.Context.Propagation;

namespace Shared.Messaging;

public static class MessagingTelemetry
{
    public static readonly ActivitySource ActivitySource = new("Messaging.Kafka");

    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    public static void InjectTraceContext(Headers headers)
    {
        InjectTraceContext(Activity.Current, headers);
    }

    public static void InjectTraceContext(Activity? activity, Headers headers)
    {
        if (activity is null)
        {
            return;
        }

        Propagator.Inject(new PropagationContext(activity.Context, default), headers, InjectHeader);
    }

    public static ActivityContext ExtractTraceContext(Headers headers) =>
        Propagator.Extract(default, headers, ExtractHeader).ActivityContext;

    public static void InjectTraceContext(IDictionary<string, string> headers)
        => InjectTraceContext(Activity.Current, headers);

    public static void InjectTraceContext(Activity? activity, IDictionary<string, string> headers)
    {
        if (activity is null)
        {
            return;
        }

        Propagator.Inject(new PropagationContext(activity.Context, default), headers, (h, key, value) => h[key] = value);
    }

    public static ActivityContext ExtractTraceContext(IDictionary<string, string> headers) =>
        Propagator.Extract(default, headers, (h, key) => h.TryGetValue(key, out var value) ? [value] : []).ActivityContext;

    public static Headers ToKafkaHeaders(IDictionary<string, string> headers)
    {
        var result = new Headers();

        foreach (var (key, value) in headers)
        {
            result.Add(key, Encoding.UTF8.GetBytes(value));
        }

        return result;
    }

    private static void InjectHeader(Headers headers, string key, string value)
        => headers.Add(key, Encoding.UTF8.GetBytes(value));

    private static IEnumerable<string> ExtractHeader(Headers headers, string key)
        => headers.TryGetLastBytes(key, out var bytes) ? [Encoding.UTF8.GetString(bytes)] : [];
}
