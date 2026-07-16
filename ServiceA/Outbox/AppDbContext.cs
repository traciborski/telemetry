using Microsoft.EntityFrameworkCore;
using Shared.Outbox;

namespace ServiceA.Outbox;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options), IOutboxDbContext
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>().HasIndex(m => m.PublishedAt);
    }
}
