using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Messaging;

namespace Shared.Outbox;

public sealed class OutboxWorkerBackgroundService<TDbContext>(IServiceScopeFactory scopeFactory, KafkaProducer producer)
    : BackgroundService
    where TDbContext : DbContext, IOutboxDbContext
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);
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

        var pending = await db.OutboxMessages
            .Where(m => m.PublishedAt == null)
            .OrderBy(m => m.CreatedAt)
            .Take(BatchSize)
            .ToListAsync(stoppingToken);

        foreach (var message in pending)
        {
            try
            {
                var payload = JsonSerializer.Deserialize<object>(message.Payload) ?? throw new InvalidOperationException($"Outbox message {message.Id} has an invalid payload");
                var headers = KafkaTelemetry.DeserializeHeaders(message.Headers);
                await producer.PublishAsync(message.Topic, message.Key, payload, headers, stoppingToken);

                message.PublishedAt = DateTimeOffset.UtcNow;
                await db.SaveChangesAsync(stoppingToken);
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                // Leave PublishedAt unset so the message is retried on the next poll.
            }
        }
    }
}