namespace ServiceC;

/// <summary>
/// BUG (intentional, for the multi-tenancy PoC): this is registered as a singleton and holds
/// mutable per-request state in an instance field. Under parallel batch processing
/// (see <see cref="Shared.Messaging.KafkaConsumerWorker{TValue}"/>, which processes
/// different partition/key groups concurrently via Task.Run), concurrent calls to
/// <see cref="OrderProcessedConsumer.Handle"/> race on <see cref="CurrentTenantId"/>:
/// one group can overwrite it while another is awaiting an I/O call, so the value read
/// after the await may belong to a different tenant than the one that started the call.
/// This class exists to demonstrate that bug and how the tenant guardrail catches it.
/// </summary>
public sealed class TenantAccessor
{
    public string? CurrentTenantId { get; set; }
}
