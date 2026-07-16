using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using OpenTelemetry.Context.Propagation;

namespace Shared.Messaging;

public static class KafkaTelemetry
{
    public static readonly ActivitySource ActivitySource = new("Messaging.Kafka"); // MyCompany.MyProduct.MyLibrary

    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

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

    public static string SerializeHeaders(Headers headers)
    {
        var entries = headers.Select(header => new HeaderEntry(header.Key, Convert.ToBase64String(header.GetValueBytes()))).ToArray();
        return JsonSerializer.Serialize(entries);
    }

    public static Headers DeserializeHeaders(string serialized)
    {
        var headers = new Headers();
        var entries = JsonSerializer.Deserialize<HeaderEntry[]>(serialized) ?? [];

        foreach (var entry in entries)
        {
            headers.Add(entry.Key, Convert.FromBase64String(entry.Value));
        }

        return headers;
    }

    private static void InjectHeader(Headers headers, string key, string value)
        => headers.Add(key, Encoding.UTF8.GetBytes(value));

    private static IEnumerable<string> ExtractHeader(Headers headers, string key)
        => headers.TryGetLastBytes(key, out var bytes) ? [Encoding.UTF8.GetString(bytes)] : [];

    private sealed record HeaderEntry(string Key, string Value);
}
