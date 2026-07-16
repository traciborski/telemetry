using Microsoft.EntityFrameworkCore;

namespace Shared.Outbox;

public interface IOutboxDbContext
{
    DbSet<OutboxMessage> OutboxMessages { get; }
}
