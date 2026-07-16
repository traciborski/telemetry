using System.Diagnostics;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using Shared.Messaging;

namespace Shared.Outbox;

public sealed class OutboxWorkerBackgroundService<TDbContext>(IServiceScopeFactory scopeFactory, KafkaProducer producer)
    : BackgroundService
    where TDbContext : DbContext, IOutboxDbContext
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);
    private const int BatchSize = 20;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await PublishPendingAsync(stoppingToken);

            try
            {
                await Task.Delay(PollInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task PublishPendingAsync(CancellationToken stoppingToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();

        List<TransactionalOutbox> pending;
        using (SuppressInstrumentationScope.Begin())
        {
            pending = await db.OutboxMessages.OrderBy(m => m.CreatedAt).Take(BatchSize).ToListAsync(stoppingToken);
        }

        if (pending.Count == 0)
        {
            return;
        }

        foreach (var message in pending)
        {
            await RelayMessageAsync(db, message, stoppingToken);
        }
    }

    private async Task RelayMessageAsync(TDbContext db, TransactionalOutbox message, CancellationToken stoppingToken)
    {
        var parentContext = MessagingTelemetry.ExtractTraceContext(message.Headers);
        using var activity = MessagingTelemetry.ActivitySource.StartActivity($"{message.Topic} outbox relay", ActivityKind.Internal, parentContext);

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
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            // Leave the message in the table so it is retried on the next poll.
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
        }
    }
}