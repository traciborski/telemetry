using System.Diagnostics.Metrics;

namespace Shared.Outbox;

public static class OutboxTelemetry
{
    public const string MeterName = "Shared.Outbox";

    private static readonly Meter Meter = new(MeterName);

    public static ObservableGauge<double> Track(Func<double> observeValue) => Meter.CreateObservableGauge("outbox.oldest_pending_age", observeValue, unit: "s", description: "Age of the oldest unpublished outbox message; 0 when the outbox is empty.", tags: []);
}