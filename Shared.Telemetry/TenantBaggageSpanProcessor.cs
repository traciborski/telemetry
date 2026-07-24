using System.Diagnostics;
using OpenTelemetry;

namespace Shared.Telemetry;

public sealed class TenantBaggageSpanProcessor : BaseProcessor<Activity>
{
    public override void OnEnd(Activity activity)
    {
        var tenantId = Baggage.GetBaggage(TenantTelemetryExtensions.TenantIdAttributeName);
        if (!string.IsNullOrWhiteSpace(tenantId))
        {
            activity.SetTag(TenantTelemetryExtensions.TenantIdAttributeName, tenantId);
        }
    }
}
