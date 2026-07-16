namespace Shared.Outbox;

public sealed class OutboxMessage
{
    public Guid Id { get; init; }
    public required string Topic { get; init; }
    public required string Key { get; init; }
    public required string Payload { get; init; }
    public required string Headers { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? PublishedAt { get; set; }
}