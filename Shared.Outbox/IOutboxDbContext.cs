using Microsoft.EntityFrameworkCore;

namespace Shared.Outbox;

public interface IOutboxDbContext
{
    DbSet<TransactionalOutbox> OutboxMessages { get; }
}
