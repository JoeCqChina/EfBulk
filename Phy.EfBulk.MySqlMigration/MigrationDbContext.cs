using Microsoft.EntityFrameworkCore;
using Phy.EfBulk.TestDb;
using System;

namespace Phy.EfBulk.MySqlMigration
{
    public class MigrationDbContext : MyDbContext
    {
        public MigrationDbContext() : base()
        {
        }


        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseMySql("Server=192.168.1.254;Port=3306;Database=bulktestedb;Uid=cvuser;Pwd=123456;Pooling=true;Max Pool Size=99;Min Pool Size=10;SslMode=Preferred;CharSet=utf8mb4;Allow User Variables=True;", new MySqlServerVersion("8.0.24"));
            base.OnConfiguring(optionsBuilder);
        }
    }
}
