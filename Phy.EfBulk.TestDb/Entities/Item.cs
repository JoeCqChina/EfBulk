using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Phy.EfBulk.TestDb.Entities
{
    public class Item
    {
        public long Id { get; set; }

        [MaxLength(128)]
        public string Token { get; set; }

        public DateTime CreatedAt { get; set; }

        public long CategoryId { get; set; }

        public virtual Category Category { get; set; }
    }
}
