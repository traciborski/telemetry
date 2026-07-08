namespace Shared.Messaging.Contracts;

/// <summary>HTTP request body accepted by ServiceA on POST /orders.</summary>
public record OrderRequest(string Product, int Quantity);
