namespace ServiceA;

public sealed class Order
{
    public Guid Id { get; init; }
    public required string Product { get; init; }
    public required int Quantity { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
}
