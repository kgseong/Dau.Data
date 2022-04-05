using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dau.Data
{
    public class EntityProperty
    {
        public int Index { get; set; }
        public string Name { get; set; }
        public string DisplayName { get; set; }
        public bool isPK { get; set; }
        public string DisplayFormat { get; set; }
        public object Value { get; set; }
    }
}
