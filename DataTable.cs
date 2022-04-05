using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dau.Data
{
    public class DataTable 
    {
        List<DataColumn> _Columns = new List<DataColumn>();
        string _Owner;
        string _Name;

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
        public List<DataColumn> Columns { get { return _Columns; } }

    }
}
