namespace Shared.Messaging.Contracts;

/// <summary>Published by ServiceA to the "orders.created" topic, consumed by ServiceB.</summary>
public record OrderCreatedMessage(Guid OrderId, string Product, int Quantity, DateTimeOffset CreatedAt);
