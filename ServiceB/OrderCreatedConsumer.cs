using Shared.Messaging;
using Shared.Messaging.Contracts;

namespace ServiceB;

public sealed class OrderCreatedConsumer : KafkaConsumerBackgroundService<OrderCreatedMessage>
{
    private readonly KafkaProducer<OrderProcessedMessage> _producer;
    private readonly ILogger<OrderCreatedConsumer> _logger;

    public OrderCreatedConsumer(IConfiguration configuration, KafkaProducer<OrderProcessedMessage> producer, ILogger<OrderCreatedConsumer> logger)
        : base(configuration["Kafka:BootstrapServers"], groupId: "service-b", topic: KafkaTopics.OrdersCreated)
    {
        _producer = producer;
        _logger = logger;
    }

    protected override async Task HandleAsync(OrderCreatedMessage message, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing order {OrderId} ({Product} x{Quantity})", message.OrderId, message.Product, message.Quantity);
        await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
        var processed = new OrderProcessedMessage(message.OrderId, message.Product, message.Quantity, DateTimeOffset.UtcNow);
        await _producer.PublishAsync(KafkaTopics.OrdersProcessed, processed.OrderId.ToString(), processed, cancellationToken);
    }
}
