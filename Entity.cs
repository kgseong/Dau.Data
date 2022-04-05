using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dau.Data
{
    public class Entity 
    {
        List<EntityProperty> _Columns = new List<EntityProperty>();
        string _Owner=null;
        string _Name=null;

        public string Owner { get { return _Owner; } set { _Owner = value; } }
        public string Name { get { return _Name; } set { _Name = value; } }
        public string TableName
        {
            get
            {
                if (Owner == null) return Name;
                return Owner + "." + Name;
            }
        }
        public List<EntityProperty> Columns { get { return _Columns; } }

    }
}
