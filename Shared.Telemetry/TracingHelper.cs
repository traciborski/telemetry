using System.Diagnostics;
using OpenTelemetry;

namespace Shared.Telemetry;

public static class TracingHelper
{
    public const string ActivitySourceName = "Shared.Telemetry.TracingHelper";

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    public static Activity? StartSpan(string? tenantId, string name, ActivityKind kind = ActivityKind.Internal, params (string Key, object? Value)[] attributes)
    {
        var activity = ActivitySource.StartActivity(name, kind);
        activity?.SetTag(TenantTelemetryExtensions.TenantIdAttributeName, tenantId);
        foreach (var (key, value) in attributes)
        {
            activity?.SetTag(key, value);
        }

        var ambientTenantId = Baggage.GetBaggage(TenantTelemetryExtensions.TenantIdAttributeName);
        if (tenantId is not null && !string.Equals(tenantId, ambientTenantId, StringComparison.Ordinal))
        {
            var ex = new InvalidOperationException($"Tenant mismatch in trace: span '{name}' was started for tenant '{tenantId}' but current trace tenant is '{ambientTenantId}'.");
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            activity?.Dispose();
            throw ex;
        }

        return activity;
    }

    public static void RecordEvent(string? tenantId, string message, params (string Key, object? Value)[] attributes)
    {
        var ambientTenantId = Baggage.GetBaggage(TenantTelemetryExtensions.TenantIdAttributeName);
        if (tenantId is not null && !string.Equals(tenantId, ambientTenantId, StringComparison.Ordinal))
        {
            var ex = new InvalidOperationException($"Tenant mismatch in trace: event '{message}' was recorded for tenant '{tenantId}' but current trace tenant is '{ambientTenantId}'.");
            Activity.Current?.SetStatus(ActivityStatusCode.Error, ex.Message);
            Activity.Current?.AddException(ex);
            throw ex;
        }

        var tags = new ActivityTagsCollection { [TenantTelemetryExtensions.TenantIdAttributeName] = tenantId };
        foreach (var (key, value) in attributes)
        {
            tags[key] = value;
        }

        Activity.Current?.AddEvent(new ActivityEvent(message, tags: tags));
    }
}
