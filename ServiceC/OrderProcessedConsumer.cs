using System.Diagnostics;
using Shared.Messaging;
using Shared.Messaging.Contracts;
using Elastic.Clients.Elasticsearch;

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
        // BUG (intentional): caching the "current" tenant on a shared singleton instead of
        // relying on the ambient Activity/Baggage. Parallel batch groups (different tenants)
        // can overwrite this field while this call is suspended on an await below.
        _tenantAccessor.CurrentTenantId = Activity.Current?.GetBaggageItem(MessagingTelemetry.TenantIdAttributeName);

        _logger.LogInformation("Forwarding processed order {OrderId} ({Product} x{Quantity}) to ServiceD", message.OrderId, message.Product, message.Quantity);
        var client = _httpClientFactory.CreateClient("ServiceD");
        var response = await client.PostAsJsonAsync("/notifications", message, cancellationToken);
        response.EnsureSuccessStatusCode();
        _logger.LogInformation("ServiceD acknowledged order {OrderId}", message.OrderId);

        var indexResponse = await _elasticsearchClient.IndexAsync(message, idx => idx.Index("orders-processed"), cancellationToken);
        if (!indexResponse.IsValidResponse)
        {
            throw new InvalidOperationException(
                $"Failed to index order {message.OrderId} in Elasticsearch: {indexResponse.DebugInformation}");
        }

        _logger.LogInformation("Indexed order {OrderId} in Elasticsearch (doc id {DocumentId})", message.OrderId, indexResponse.Id);

        // Guardrail: the cached tenant read back here may no longer match the tenant of *this*
        // logical call, because the singleton field can have been overwritten by another
        // concurrently-processed batch group in between. We compare it against the ambient
        // trace tenant (the source of truth for this call) and surface any drift as a failed
        // span + error log, instead of silently using the (possibly wrong) cached tenant.
        var cachedTenantId = _tenantAccessor.CurrentTenantId;
        var currentTenantId = Activity.Current?.GetBaggageItem(MessagingTelemetry.TenantIdAttributeName);
        if (!string.Equals(cachedTenantId, currentTenantId, StringComparison.Ordinal))
        {
            var ex = new InvalidOperationException(
                $"Tenant mismatch in trace for order {message.OrderId}: cached tenant '{cachedTenantId}' (TenantAccessor) does not match current tenant '{currentTenantId}' (trace) - likely a race between concurrently processed batch groups.");
            Activity.Current?.SetStatus(ActivityStatusCode.Error, ex.Message);
            Activity.Current?.AddException(ex);
            _logger.LogError(ex, "Tenant mismatch detected for order {OrderId}: cached={CachedTenantId} current={CurrentTenantId}", message.OrderId, cachedTenantId, currentTenantId);
            throw ex;
        }
    }
}
