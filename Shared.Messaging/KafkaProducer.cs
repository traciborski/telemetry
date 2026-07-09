using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Shared.Messaging;

public sealed class KafkaProducer<TValue> : IDisposable
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
    }

    public void Dispose() => _producer.Dispose();
}
