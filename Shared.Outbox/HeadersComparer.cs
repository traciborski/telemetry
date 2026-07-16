using Microsoft.EntityFrameworkCore.ChangeTracking;

namespace Shared.Outbox;

public class HeadersComparer : ValueComparer<Dictionary<string, string>>
{
    public HeadersComparer()
        : base(favorStructuralComparisons: true)
    {
    }
}