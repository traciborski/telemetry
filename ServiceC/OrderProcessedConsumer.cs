using System.Diagnostics;
using Shared.Messaging;
using Shared.Messaging.Contracts;
using Shared.Telemetry;
using Elastic.Clients.Elasticsearch;
using OpenTelemetry;

namespace ServiceC;

public sealed class OrderProcessedConsumer : KafkaConsumerWorker<OrderProcessedMessage>
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ElasticsearchClient _elasticsearchClient;
    private readonly ILogger<OrderProcessedConsumer> _logger;
    private readonly TenantAccessor _tenantAccessor;

    public OrderProcessedConsumer(IConfiguration configuration, IHttpClientFactory httpClientFactory, ElasticsearchClient elasticsearchClient, ILogger<OrderProcessedConsumer> logger, TenantAccessor tenantAccessor)
        : base(configuration["Kafka:BootstrapServers"], groupId: "service-c", topic: KafkaTopics.OrdersProcessed)
    {
        _httpClientFactory = httpClientFactory;
        _elasticsearchClient = elasticsearchClient;
        _logger = logger;
        _tenantAccessor = tenantAccessor;
    }

    protected override async Task Handle(OrderProcessedMessage message, CancellationToken cancellationToken)
    {
        _tenantAccessor.CurrentTenantId = Baggage.GetBaggage(MessagingTelemetry.TenantIdAttributeName);

        await Task.Yield();

        _logger.LogInformation("Forwarding processed order {OrderId} ({Product} x{Quantity}) to ServiceD", message.OrderId, message.Product, message.Quantity);
        var client = _httpClientFactory.CreateClient("ServiceD");
        var response = await client.PostAsJsonAsync("/notifications", message, cancellationToken);
        response.EnsureSuccessStatusCode();
        _logger.LogInformation("ServiceD acknowledged order {OrderId}", message.OrderId);

        using var indexActivity = TracingHelper.StartSpan(_tenantAccessor.CurrentTenantId, "elasticsearch.index_order", ActivityKind.Client, ("order.id", message.OrderId));

        var indexResponse = await _elasticsearchClient.IndexAsync(message, idx => idx.Index("orders-processed"), cancellationToken);
        if (!indexResponse.IsValidResponse)
        {
            throw new InvalidOperationException($"Failed to index order {message.OrderId} in Elasticsearch: {indexResponse.DebugInformation}");
        }

        _logger.LogInformation("Indexed order {OrderId} in Elasticsearch (doc id {DocumentId})", message.OrderId, indexResponse.Id);
    }
}
