using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Messaging;

/// <summary>
/// Base class for a background Kafka consumer. Subclasses only implement what happens with a
/// deserialized message; subscribing, polling, trace-context extraction and offset commits all
/// happen here so every consuming service behaves the same way.
/// </summary>
public abstract class KafkaConsumerBackgroundService<TValue> : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topic;
    private readonly ILogger _logger;

    protected KafkaConsumerBackgroundService(string bootstrapServers, string groupId, string topic, ILogger logger)
    {
        _topic = topic;
        _logger = logger;
        _consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
    }

    protected abstract Task HandleAsync(TValue message, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);

        // Confluent.Kafka's consumer has no true async Consume API; a short blocking poll on a
        // dedicated BackgroundService thread is the standard pattern recommended by Confluent.
        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result;
            try
            {
                result = _consumer.Consume(TimeSpan.FromSeconds(1));
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Error consuming from topic {Topic}", _topic);
                continue;
            }

            if (result?.Message is null)
            {
                continue;
            }

            var parentContext = KafkaTelemetry.ExtractTraceContext(result.Message.Headers);
            using var activity = KafkaTelemetry.ActivitySource.StartActivity(
                $"{_topic} process", ActivityKind.Consumer, parentContext);

            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.destination.name", _topic);
            activity?.SetTag("messaging.operation", "process");
            activity?.SetTag("messaging.kafka.message.key", result.Message.Key);

            try
            {
                var value = JsonSerializer.Deserialize<TValue>(result.Message.Value)
                    ?? throw new InvalidOperationException($"Could not deserialize message from topic {_topic}");

                await HandleAsync(value, stoppingToken);
                _consumer.Commit(result);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                _logger.LogError(ex, "Error handling message (key={Key}) from topic {Topic}", result.Message.Key, _topic);
            }
        }

        _consumer.Close();
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }
}
