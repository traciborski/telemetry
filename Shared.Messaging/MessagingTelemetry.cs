using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Shared.Messaging;

public static class MessagingTelemetry
{
    public static readonly ActivitySource ActivitySource = new("Messaging.Kafka");
    public const string TenantIdAttributeName = "tenant.id";
    public const string TenantIdHeaderName = "tenant-id";

    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    public static void InjectTraceContext(Headers headers)
    {
        InjectTraceContext(Activity.Current, headers);
    }

    public static void InjectTraceContext(Activity? activity, Headers headers)
    {
        var currentActivity = activity ?? throw new InvalidOperationException($"Missing required tenant context '{TenantIdHeaderName}'.");
        var tenantId = RequireTenantId(currentActivity);

        Propagator.Inject(new PropagationContext(currentActivity.Context, Baggage.Current), headers, InjectHeader);
        headers.Remove(TenantIdHeaderName);
        headers.Add(TenantIdHeaderName, Encoding.UTF8.GetBytes(tenantId));
    }

    public static ActivityContext ExtractTraceContext(Headers headers) =>
        Propagator.Extract(default, headers, ExtractHeader).ActivityContext;

    public static void InjectTraceContext(IDictionary<string, string> headers)
        => InjectTraceContext(Activity.Current, headers);

    public static void InjectTraceContext(Activity? activity, IDictionary<string, string> headers)
    {
        var currentActivity = activity ?? throw new InvalidOperationException($"Missing required tenant context '{TenantIdHeaderName}'.");
        var tenantId = RequireTenantId(currentActivity);

        Propagator.Inject(new PropagationContext(currentActivity.Context, Baggage.Current), headers, (h, key, value) => h[key] = value);
        headers[TenantIdHeaderName] = tenantId;
    }

    public static ActivityContext ExtractTraceContext(IDictionary<string, string> headers) =>
        Propagator.Extract(default, headers, (h, key) => h.TryGetValue(key, out var value) ? [value] : []).ActivityContext;

    public static PropagationContext ExtractPropagationContext(Headers headers) =>
        Propagator.Extract(default, headers, ExtractHeader);

    public static PropagationContext ExtractPropagationContext(IDictionary<string, string> headers) =>
        Propagator.Extract(default, headers, (h, key) => h.TryGetValue(key, out var value) ? [value] : []);

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

    public static string? ExtractTenantId(Headers headers)
        => ExtractTenantId(headers.TryGetLastBytes(TenantIdHeaderName, out var bytes) ? Encoding.UTF8.GetString(bytes) : null);

    public static string? ExtractTenantId(IDictionary<string, string> headers)
    {
        headers.TryGetValue(TenantIdHeaderName, out var tenantId);
        return ExtractTenantId(tenantId);
    }

    public static void ApplyTenantContext(Activity? activity, Headers headers)
    {
        _ = activity ?? throw new InvalidOperationException($"Missing required tenant context '{TenantIdHeaderName}'.");
        var propagationContext = ExtractPropagationContext(headers);
        var tenantId = RequireTenantId(headers, propagationContext);
        Baggage.SetBaggage(TenantIdAttributeName, tenantId);
    }

    public static void ApplyTenantContext(Activity? activity, IDictionary<string, string> headers)
    {
        _ = activity ?? throw new InvalidOperationException($"Missing required tenant context '{TenantIdHeaderName}'.");
        var propagationContext = ExtractPropagationContext(headers);
        var tenantId = RequireTenantId(headers, propagationContext);
        Baggage.SetBaggage(TenantIdAttributeName, tenantId);
    }

    public static void ApplyTenantContext(Activity? activity)
    {
        var currentActivity = activity ?? throw new InvalidOperationException($"Missing required tenant context '{TenantIdHeaderName}'.");
        var tenantId = RequireTenantId(currentActivity);
        Baggage.SetBaggage(TenantIdAttributeName, tenantId);
    }

    private static string? ExtractTenantId(string? tenantId)
        => string.IsNullOrWhiteSpace(tenantId) ? null : tenantId;

    private static string? GetTenantIdFromActivity(Activity activity)
        => Baggage.GetBaggage(TenantIdAttributeName);

    private static string RequireTenantId(Activity activity)
        => GetTenantIdFromActivity(activity)
           ?? throw new InvalidOperationException($"Missing required tenant context '{TenantIdHeaderName}'.");

    private static string RequireTenantId(Headers headers)
        => ExtractTenantId(headers) ?? throw new InvalidOperationException($"Missing required tenant header '{TenantIdHeaderName}'.");

    private static string RequireTenantId(IDictionary<string, string> headers)
        => ExtractTenantId(headers) ?? throw new InvalidOperationException($"Missing required tenant header '{TenantIdHeaderName}'.");

    private static string RequireTenantId(Headers headers, PropagationContext propagationContext)
    {
        var tenantId = RequireTenantId(headers);
        EnsureTenantMatchesPropagationContext(tenantId, propagationContext);
        return tenantId;
    }

    private static string RequireTenantId(IDictionary<string, string> headers, PropagationContext propagationContext)
    {
        var tenantId = RequireTenantId(headers);
        EnsureTenantMatchesPropagationContext(tenantId, propagationContext);
        return tenantId;
    }

    private static void EnsureTenantMatchesPropagationContext(string tenantId, PropagationContext propagationContext)
    {
        var currentTenantId = propagationContext.Baggage.GetBaggage(TenantIdAttributeName);
        if (!string.IsNullOrWhiteSpace(currentTenantId) && !string.Equals(currentTenantId, tenantId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Tenant mismatch in trace: current tenant '{currentTenantId}' does not match incoming tenant '{tenantId}'.");
        }
    }
}
