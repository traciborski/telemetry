using Shared.Messaging;
using Shared.Messaging.Contracts;
using Elastic.Clients.Elasticsearch;

namespace ServiceC;

public sealed class OrderProcessedConsumer : KafkaConsumerBackgroundService<OrderProcessedMessage>
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ElasticsearchClient _elasticsearchClient;
    private readonly ILogger<OrderProcessedConsumer> _logger;

    public OrderProcessedConsumer(IConfiguration configuration, IHttpClientFactory httpClientFactory, ElasticsearchClient elasticsearchClient, ILogger<OrderProcessedConsumer> logger)
        : base(configuration["Kafka:BootstrapServers"], groupId: "service-c", topic: KafkaTopics.OrdersProcessed)
    {
        _httpClientFactory = httpClientFactory;
        _elasticsearchClient = elasticsearchClient;
        _logger = logger;
    }

    protected override async Task HandleAsync(OrderProcessedMessage message, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Forwarding processed order {OrderId} ({Product} x{Quantity}) to ServiceD", message.OrderId, message.Product, message.Quantity);
        var client = _httpClientFactory.CreateClient("ServiceD");
        var response = await client.PostAsJsonAsync("/notifications", message, cancellationToken);
        response.EnsureSuccessStatusCode();
        _logger.LogInformation("ServiceD acknowledged order {OrderId}", message.OrderId);

        var indexResponse = await _elasticsearchClient.IndexAsync(message, idx => idx.Index("orders-processed"), cancellationToken);
        if (indexResponse.IsValidResponse)
        {
            _logger.LogInformation("Indexed order {OrderId} in Elasticsearch (doc id {DocumentId})", message.OrderId, indexResponse.Id);
        }
        else
        {
            _logger.LogWarning("Failed to index order {OrderId} in Elasticsearch: {Error}", message.OrderId, indexResponse.DebugInformation);
        }
    }
}
