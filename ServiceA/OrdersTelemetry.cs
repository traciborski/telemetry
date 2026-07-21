using System.Diagnostics.Metrics;

namespace ServiceA;

public static class OrdersTelemetry
{
    public const string MeterName = "ServiceA.Orders";

    private static readonly Meter Meter = new(MeterName);

    public static readonly Counter<long> OrdersCreated = Meter.CreateCounter<long>(
        "orders.created",
        unit: "{order}",
        description: "Number of orders successfully created via POST /orders.");
}
