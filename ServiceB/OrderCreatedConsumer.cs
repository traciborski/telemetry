using Shared.Messaging;
using Shared.Messaging.Contracts;
using StackExchange.Redis;

namespace ServiceB;

public sealed class OrderCreatedConsumer : KafkaConsumerBackgroundService<OrderCreatedMessage>
{
    private const string ProcessedCounterKey = "orders:processed:count";

    private readonly KafkaProducer<OrderProcessedMessage> _producer;
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<OrderCreatedConsumer> _logger;

    public OrderCreatedConsumer(IConfiguration configuration, KafkaProducer<OrderProcessedMessage> producer, IConnectionMultiplexer redis, ILogger<OrderCreatedConsumer> logger)
        : base(configuration["Kafka:BootstrapServers"], groupId: "service-b", topic: KafkaTopics.OrdersCreated)
    {
        _producer = producer;
        _redis = redis;
        _logger = logger;
    }

    protected override async Task Handle(OrderCreatedMessage message, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Processing order {OrderId} ({Product} x{Quantity})", message.OrderId, message.Product, message.Quantity);
        await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);

        var processedCount = await _redis.GetDatabase().StringIncrementAsync(ProcessedCounterKey);
        _logger.LogInformation("Redis counter {CounterKey} is now {ProcessedCount}", ProcessedCounterKey, processedCount);

        var processed = new OrderProcessedMessage(message.OrderId, message.Product, message.Quantity, DateTimeOffset.UtcNow);
        await _producer.PublishAsync(KafkaTopics.OrdersProcessed, processed.OrderId.ToString(), processed, cancellationToken);
    }
}
