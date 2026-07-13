namespace Shared.Messaging.Contracts;

public record OrderProcessedMessage(Guid OrderId, string Product, int Quantity, DateTimeOffset ProcessedAt);
