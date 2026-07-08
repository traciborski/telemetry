using System.Net.Http.Json;
using Shared.Messaging;
using Shared.Messaging.Contracts;

namespace ServiceC;

/// <summary>Consumes "orders.processed" and forwards the order to ServiceD over HTTP.</summary>
public sealed class OrderProcessedConsumer : KafkaConsumerBackgroundService<OrderProcessedMessage>
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<OrderProcessedConsumer> _logger;

    public OrderProcessedConsumer(
        IConfiguration configuration,
        IHttpClientFactory httpClientFactory,
        ILogger<OrderProcessedConsumer> logger)
        : base(
            configuration["Kafka:BootstrapServers"] ?? "redpanda:9092",
            groupId: "service-c",
            topic: KafkaTopics.OrdersProcessed,
            logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    protected override async Task HandleAsync(OrderProcessedMessage message, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Forwarding processed order {OrderId} ({Product} x{Quantity}) to ServiceD",
            message.OrderId, message.Product, message.Quantity);

        var client = _httpClientFactory.CreateClient("ServiceD");
        var response = await client.PostAsJsonAsync("/notifications", message, cancellationToken);
        response.EnsureSuccessStatusCode();

        _logger.LogInformation("ServiceD acknowledged order {OrderId}", message.OrderId);
    }
}
