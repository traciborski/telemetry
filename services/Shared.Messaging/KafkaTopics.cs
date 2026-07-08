namespace Shared.Messaging;

/// <summary>Well-known Kafka topic names shared by producers and consumers.</summary>
public static class KafkaTopics
{
    public const string OrdersCreated = "orders.created";
    public const string OrdersProcessed = "orders.processed";
}
