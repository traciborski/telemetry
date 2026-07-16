using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Shared.Outbox;

public sealed class TransactionalOutboxConfiguration : IEntityTypeConfiguration<TransactionalOutbox>
{
    public void Configure(EntityTypeBuilder<TransactionalOutbox> builder)
    {
        builder.ToTable("TransactionalOutbox", TransactionalOutboxConstants.Schema, t =>
        {
            t.HasCheckConstraint("transactionaloutboxheaders_mustbejson", "(ISNULL(ISJSON([Headers]), 1) > 0)");
        }).HasKey(t => t.Id);

        builder.Property(t => t.Payload).HasColumnType("nvarchar(max)").IsRequired();
        builder.Property(t => t.Topic).HasMaxLength(200).IsRequired();
        builder.Property(t => t.Key).HasMaxLength(200).IsRequired(required: false)
            .HasDefaultValue(null);
        builder.Property(t => t.CreatedAt).IsRequired().HasDefaultValueSql("GETDATE()");
        builder.Property(t => t.Headers).IsRequired(required: false).HasColumnType("nvarchar(max)")
            .HasDefaultValue(null)
            .HasConversion<HeadersConverter, HeadersComparer>();
    }
}
