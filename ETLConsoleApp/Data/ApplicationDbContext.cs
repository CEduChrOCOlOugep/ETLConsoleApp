using Microsoft.EntityFrameworkCore;

namespace ETLConsoleApp.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options) { }

        public DbSet<DataIntegration> DataIntegration { get; set; }
    }

    public class DataIntegration
    {
        public string PERN { get; set; }
        public string Field1 { get; set; }
        public string Field2 { get; set; }
        // Add other fields as necessary
    }
}