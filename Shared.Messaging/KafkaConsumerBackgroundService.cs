using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace Shared.Messaging;

public abstract class KafkaConsumerBackgroundService<TValue> : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topic;

    protected KafkaConsumerBackgroundService(string? bootstrapServers, string groupId, string topic)
    {
        _topic = topic;
        _consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers ?? throw new Exception("no kafka"),
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
    }

    protected abstract Task HandleAsync(TValue message, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? result;
            try
            {
                result = _consumer.Consume(TimeSpan.FromSeconds(1));
            }
            catch (ConsumeException ex)
            {
                using var infraActivity = KafkaTelemetry.ActivitySource.StartActivity($"{_topic} consume_error", ActivityKind.Consumer);
                infraActivity?.SetStatus(ActivityStatusCode.Error, "Kafka connection or consumption failed");
                infraActivity?.AddException(ex);
                infraActivity?.SetTag("messaging.destination.name", _topic);
                infraActivity?.SetTag("error.type", ex.Error.Code.ToString());
                continue;
            }

            if (result?.Message is null)
            {
                continue;
            }

            var parentContext = KafkaTelemetry.ExtractTraceContext(result.Message.Headers);
            using var activity = KafkaTelemetry.ActivitySource.StartActivity($"{_topic} process", ActivityKind.Consumer, parentContext);

            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.destination.name", _topic);
            activity?.SetTag("messaging.operation", "process");
            activity?.SetTag("messaging.kafka.message.key", result.Message.Key);

            try
            {
                var value = JsonSerializer.Deserialize<TValue>(result.Message.Value) ?? throw new InvalidOperationException($"Could not deserialize message from topic {_topic}");

                await HandleAsync(value, stoppingToken);
                _consumer.Commit(result);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity?.AddException(ex);
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
