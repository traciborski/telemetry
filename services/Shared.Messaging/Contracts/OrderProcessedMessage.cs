namespace Shared.Messaging.Contracts;

/// <summary>
/// Published by ServiceB to the "orders.processed" topic, consumed by ServiceC and forwarded as-is
/// over HTTP to ServiceD.
/// </summary>
public record OrderProcessedMessage(Guid OrderId, string Product, int Quantity, DateTimeOffset ProcessedAt);
