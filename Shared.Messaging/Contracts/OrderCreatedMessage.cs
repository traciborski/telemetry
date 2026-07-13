namespace Shared.Messaging.Contracts;

public record OrderCreatedMessage(Guid OrderId, string Product, int Quantity, DateTimeOffset CreatedAt);
