using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace Shared.Messaging;

public abstract class KafkaConsumerBackgroundService<TValue> : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topic;
    private readonly int _batchSize = 4;
    private readonly TimeSpan _batchFillTimeout = TimeSpan.FromMilliseconds(100);

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

    protected abstract Task Handle(TValue message, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var batch = ConsumeBatch(stoppingToken);

                if (batch.Count > 0)
                {
                    await ProcessBatch(batch, stoppingToken);
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    private List<ConsumeResult<string, string>> ConsumeBatch(CancellationToken stoppingToken)
    {
        var batch = new List<ConsumeResult<string, string>>(_batchSize);

        while (batch.Count < _batchSize && !stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(_batchFillTimeout);
                if (result?.Message is null)
                {
                    break;
                }

                batch.Add(result);
            }
            catch (ConsumeException ex)
            {
                RecordConsumeError(ex);
            }
        }

        return batch;
    }

    private void RecordConsumeError(ConsumeException ex)
    {
        using var infraActivity = MessagingTelemetry.ActivitySource.StartActivity($"{_topic} consume_error", ActivityKind.Consumer);
        infraActivity?.SetStatus(ActivityStatusCode.Error, "Kafka connection or consumption failed");
        infraActivity?.AddException(ex);
        infraActivity?.SetTag("messaging.destination.name", _topic);
        infraActivity?.SetTag("error.type", ex.Error.Code.ToString());
    }

    private async Task ProcessBatch(IReadOnlyList<ConsumeResult<string, string>> batch, CancellationToken stoppingToken)
    {
        var tasks = batch
            .GroupBy(result => new { result.TopicPartition, result.Message.Key })
            .Select(group => Task.Run(() => ProcessGroup(group, stoppingToken), stoppingToken))
            .ToArray();

        var results = (await Task.WhenAll(tasks)).SelectMany(group => group);
        CommitSuccessful(results);
    }

    private async Task<IReadOnlyList<MessageProcessingResult>> ProcessGroup(IEnumerable<ConsumeResult<string, string>> group, CancellationToken stoppingToken)
    {
        var results = new List<MessageProcessingResult>();

        foreach (var result in group)
        {
            results.Add(await ProcessMessage(result, stoppingToken));
        }

        return results;
    }

    private async Task<MessageProcessingResult> ProcessMessage(ConsumeResult<string, string> result, CancellationToken stoppingToken)
    {
        var parentContext = MessagingTelemetry.ExtractTraceContext(result.Message.Headers);
        using var activity = MessagingTelemetry.ActivitySource.StartActivity($"{_topic} process", ActivityKind.Consumer, parentContext);

        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", _topic);
        activity?.SetTag("messaging.operation", "process");
        activity?.SetTag("messaging.kafka.message.key", result.Message.Key);

        try
        {
            var value = JsonSerializer.Deserialize<TValue>(result.Message.Value)
                ?? throw new InvalidOperationException($"Could not deserialize message from topic {_topic}");
            await Handle(value, stoppingToken);
            return new MessageProcessingResult(result, Succeeded: true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            return new MessageProcessingResult(result, Succeeded: false);
        }
    }

    private void CommitSuccessful(IEnumerable<MessageProcessingResult> results)
    {
        foreach (var partitionResults in results.GroupBy(result => result.Result.TopicPartition))
        {
            ConsumeResult<string, string>? lastSuccessfulResult = null;

            foreach (var result in partitionResults.OrderBy(result => result.Result.Offset.Value))
            {
                if (!result.Succeeded)
                {
                    break;
                }

                lastSuccessfulResult = result.Result;
            }

            if (lastSuccessfulResult is not null)
            {
                _consumer.Commit(lastSuccessfulResult);
            }
        }
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }

    private sealed record MessageProcessingResult(ConsumeResult<string, string> Result, bool Succeeded);
}
