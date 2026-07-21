using System.Diagnostics.Metrics;

namespace Shared.Outbox;

public static class OutboxTelemetry
{
    public const string MeterName = "Shared.Outbox";

    public static readonly Meter Meter = new(MeterName);
}
