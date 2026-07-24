using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using Shared.Messaging;

namespace Shared.Outbox;

public sealed class OutboxWorker<TDbContext>(IServiceScopeFactory scopeFactory, KafkaProducer producer) : BackgroundService
    where TDbContext : DbContext, IOutboxDbContext
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);
    private const int BatchSize = 20;
    private static double _oldestPendingAgeSeconds;
    private static readonly ObservableGauge<double> OldestPendingAgeGauge = OutboxTelemetry.Track(() => _oldestPendingAgeSeconds);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (await PublishPendingAsync(stoppingToken))
            {
                continue;
            }

            try
            {
                await Task.Delay(PollInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task<bool> PublishPendingAsync(CancellationToken stoppingToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();

        List<TransactionalOutbox> pending;
        using (SuppressInstrumentationScope.Begin())
        {
            pending = await db.OutboxMessages.OrderBy(m => m.CreatedAt).Take(BatchSize).ToListAsync(stoppingToken);
        }

        _oldestPendingAgeSeconds = pending.Count == 0 ? 0 : (DateTimeOffset.UtcNow - pending[0].CreatedAt).TotalSeconds;

        if (pending.Count == 0)
        {
            return false;
        }

        var publishedCount = 0;
        foreach (var message in pending)
        {
            if (await RelayMessageAsync(db, message, stoppingToken))
            {
                publishedCount++;
            }
        }

        return pending.Count == BatchSize && publishedCount == pending.Count;
    }

    private async Task<bool> RelayMessageAsync(TDbContext db, TransactionalOutbox message, CancellationToken stoppingToken)
    {
        var propagationContext = MessagingTelemetry.ExtractPropagationContext(message.Headers);
        var tenantId = MessagingTelemetry.ExtractTenantId(message.Headers) ?? throw new InvalidOperationException($"Missing required tenant header '{MessagingTelemetry.TenantIdHeaderName}' on outbox message {message.Id}.");
        var propagatedTenantId = propagationContext.Baggage.GetBaggage(MessagingTelemetry.TenantIdAttributeName);
        if (string.IsNullOrWhiteSpace(propagatedTenantId))
        {
            throw new InvalidOperationException($"Missing required tenant context '{MessagingTelemetry.TenantIdAttributeName}' in trace headers on outbox message {message.Id}.");
        }

        if (!string.Equals(propagatedTenantId, tenantId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Tenant mismatch in trace on outbox message {message.Id}: propagated tenant '{propagatedTenantId}' does not match message tenant '{tenantId}'.");
        }

        var parentContext = propagationContext.ActivityContext;
        using var activity = MessagingTelemetry.ActivitySource.StartActivity($"{message.Topic} outbox relay", ActivityKind.Internal, parentContext);
        Baggage.SetBaggage(MessagingTelemetry.TenantIdAttributeName, tenantId);

        activity?.SetTag("messaging.destination.name", message.Topic);
        activity?.SetTag("messaging.kafka.message.key", message.Key);
        activity?.SetTag("outbox.message.id", message.Id);
        activity?.SetTag("outbox.queue_time_ms", (DateTimeOffset.UtcNow - message.CreatedAt).TotalMilliseconds);

        try
        {
            var payload = JsonSerializer.Deserialize<object>(message.Payload) ?? throw new InvalidOperationException($"Outbox message {message.Id} has an invalid payload");
            var headers = MessagingTelemetry.ToKafkaHeaders(message.Headers);

            MessagingTelemetry.InjectTraceContext(activity, headers);

            await producer.PublishAsync(message.Topic, message.Key, payload, headers, stoppingToken);

            db.OutboxMessages.Remove(message);
            await db.SaveChangesAsync(stoppingToken);
            return true;
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            return false;
        }
    }
}