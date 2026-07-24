using System.Diagnostics.Metrics;

namespace ServiceA;

public static class OrdersMetrics
{
    public const string MeterName = "ServiceA.Orders";

    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> OrdersCreated = Meter.CreateCounter<long>("orders.created", unit: "{order}", description: "Number of orders successfully created via POST /orders.");

    public static void Increment(string product) => OrdersCreated.Add(1, new KeyValuePair<string, object?>("product", product));
}