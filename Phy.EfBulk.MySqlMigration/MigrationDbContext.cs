using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Phy.EfBulk.TestDb
{
    public class MigrationDbContext : MyDbContext
    {
        public MigrationDbContext():base() { 
        }


        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseMySql("Server=localhost;Port=3306;Database=BulkTestDb;Uid=root;Pwd=yourpwd;Pooling=true;Max Pool Size=99;Min Pool Size=10;SslMode=Preferred;CharSet=utf8mb4;Allow User Variables=True;", new MySqlServerVersion("8.0.24"));
            base.OnConfiguring(optionsBuilder);
        }
    }
}
