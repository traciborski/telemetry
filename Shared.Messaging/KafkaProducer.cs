using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;

namespace Shared.Messaging;

public sealed class KafkaProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(string? bootstrapServers)
    {
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers ?? throw new Exception("no kafka"),
            Acks = Acks.Leader,
        }).Build();
    }

    public async Task PublishAsync(string topic, string key, object value, CancellationToken cancellationToken)
    {
        await PublishAsync(topic, key, value, new Headers(), cancellationToken);
    }

    public async Task PublishAsync(string topic, string key, object value, Headers originHeaders, CancellationToken cancellationToken)
    {
        var parentContext = KafkaTelemetry.ExtractTraceContext(originHeaders);
        using var activity = KafkaTelemetry.ActivitySource.StartActivity($"{topic} publish", ActivityKind.Producer, parentContext);
        activity?.SetTag("messaging.kafka.outbox_relayed", true);

        KafkaTelemetry.InjectTraceContext(activity, originHeaders);

        await ProduceAsync(topic, key, value, originHeaders, activity, cancellationToken);
    }

    private async Task ProduceAsync(string topic, string key, object value, Headers headers, Activity? activity, CancellationToken cancellationToken)
    {
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.operation", "publish");
        activity?.SetTag("messaging.kafka.message.key", key);

        var message = new Message<string, string>
        {
            Key = key,
            Value = JsonSerializer.Serialize(value),
            Headers = headers,
        };

        try
        {
            var result = await _producer.ProduceAsync(topic, message, cancellationToken);

            activity?.SetTag("messaging.kafka.destination.partition", result.Partition.Value);
            activity?.SetTag("messaging.kafka.message.offset", result.Offset.Value);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }

    public void Dispose() => _producer.Dispose();
}