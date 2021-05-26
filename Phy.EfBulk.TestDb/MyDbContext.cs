using Microsoft.EntityFrameworkCore;
using Phy.EfBulk.TestDb.Entities;
using System;

namespace Phy.EfBulk.TestDb
{
    public partial class MyDbContext : DbContext
    {
        public MyDbContext() { }

        public MyDbContext(DbContextOptions options) :
            base(options)
        {

        }
        public DbSet<Category> Categories { get; set; }
        public DbSet<Item> Items { get; set; }

        //public MigrationDbContext(DbContextOptions options) :
        //    base(options)
        //{

        //}

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
        }
    }
}
