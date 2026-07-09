using Microsoft.EntityFrameworkCore;

namespace ServiceD;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options);
