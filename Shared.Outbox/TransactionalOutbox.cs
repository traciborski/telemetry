namespace Shared.Outbox;

public class TransactionalOutbox
{
    public long Id { get; set; }

    public required string Topic { get; set; }

    public string? Key { get; set; }

    public required string Payload { get; set; }

    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;

    public Dictionary<string, string> Headers { get; } = new Dictionary<string, string>();
}
