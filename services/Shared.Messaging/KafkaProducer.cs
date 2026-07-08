using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Shared.Messaging;

/// <summary>
/// Thin wrapper around Confluent.Kafka's IProducer that serializes the value as JSON and wraps each
/// publish in a "publish" Activity, so it shows up as a span in the distributed trace with the trace
/// context injected into the Kafka message headers for the consumer to pick up.
/// </summary>
public sealed class KafkaProducer<TValue> : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer<TValue>> _logger;

    public KafkaProducer(string bootstrapServers, ILogger<KafkaProducer<TValue>> logger)
    {
        _logger = logger;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
        }).Build();
    }

    public async Task PublishAsync(string topic, string key, TValue value, CancellationToken cancellationToken = default)
    {
        using var activity = KafkaTelemetry.ActivitySource.StartActivity($"{topic} publish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.operation", "publish");
        activity?.SetTag("messaging.kafka.message.key", key);

        var headers = new Headers();
        KafkaTelemetry.InjectTraceContext(activity, headers);

        var message = new Message<string, string>
        {
            Key = key,
            Value = JsonSerializer.Serialize(value),
            Headers = headers,
        };

        var result = await _producer.ProduceAsync(topic, message, cancellationToken);

        activity?.SetTag("messaging.kafka.destination.partition", result.Partition.Value);
        activity?.SetTag("messaging.kafka.message.offset", result.Offset.Value);

        _logger.LogInformation(
            "Published {MessageType} (key={Key}) to topic {Topic} [partition {Partition}, offset {Offset}]",
            typeof(TValue).Name, key, topic, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose() => _producer.Dispose();
}
