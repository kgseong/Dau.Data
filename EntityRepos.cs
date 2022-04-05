using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dau.Data
{
    public static class EntityRepos
    {
        private static string GetSchemaStoreKey(Type T, string owner=null, string table=null)
        {
            if (owner == null) owner = "";
            if (table == null) table = "";

            string key = T.FullName + "." + owner + "." + table;
            return key;
        }
        static Dictionary<string, Entity> _Entities = new Dictionary<string, Entity>();
        public static bool Set<T>(Entity entity, string owner = null, string table = null, bool ForceSet = false) {
            var key = GetSchemaStoreKey(typeof(T), owner, table);
            if (_Entities.Keys.Contains(key) == true)
            {
                if (ForceSet == true)
                {
                    _Entities[key] = entity;
                    return true;
                }
                return true;
            }else
            {
                _Entities.Add(key, entity);
                return false;
            }
        }
        public static Entity Get(object entity, string owner = null, string table = null)
        {
            var t = entity.GetType();
            var key = GetSchemaStoreKey(t, owner, table);

            if (_Entities.Keys.Contains(key)==true)
            {
                return _Entities[key];
            }
            return null;
        }
        public static Entity Get<T>(string owner = null, string table = null)
        {
            var key = GetSchemaStoreKey(typeof(T), owner, table);

            if (_Entities.Keys.Contains(key) == true)
            {
                return _Entities[key];
            }
            return null;
        }
        public static bool Remove<T>(string owner = null, string table = null)
        {
            var key = GetSchemaStoreKey(typeof(T), owner, table);
            if (_Entities.Keys.Contains(key) == true)
            {
                _Entities.Remove(key);
                return true;
            }
            return false;
        }
    }
}
