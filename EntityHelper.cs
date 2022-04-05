using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;
using System.Reflection;
using log4net;

namespace Dau.Data
{
    public class EntityHelper
    {
        static ILog _Log;
        //static Dictionary<string, DataTableScheme> SchemaStore = new Dictionary<string, DataTableScheme>();

        static ILog Log
        {
            get
            {
                if (_Log == null)
                {
                    _Log = Logger.GetLogger("Dau.Data.EntityHelper");
                }
                return _Log;
            }
        }

        /*
        public static Entity GetDefaultEntity<T>(string owner = null, string table = null)
        {

            Entity tbl = EntityRepos.Get<T>(owner, table);

            if (tbl != null) return tbl;

            var t = typeof(T);
            
            string tablename = string.IsNullOrEmpty(table) ? t.Name : table;
            if (owner != null) tablename = owner + "." + tablename;

            tbl = new Entity();
            tbl.Owner = owner;
            tbl.Name = tablename;

            var props = t.GetProperties();

            if (props != null)
            {
                int idx = 0;
                foreach (var item in props)
                {
                    
                    bool is_nomap = false;
                    EntityProperty col = new EntityProperty();
                    col.Name = item.Name;
                    col.DisplayName = item.Name;
                    col.Index = idx;
                    idx += 1;
                    //col.Value = item.GetValue(source);


                    //어트리뷰트 특성값 읽어 내기
                    object[] attrs = item.GetCustomAttributes(true);
                    
                    foreach (var attr in attrs)
                    {
                        var attr_tp = attr.GetType();

                        if (attr_tp == typeof(System.ComponentModel.DataAnnotations.KeyAttribute))
                        {
                            //기본키 뽑아내기
                            System.ComponentModel.DataAnnotations.KeyAttribute key = attr as System.ComponentModel.DataAnnotations.KeyAttribute;
                            if (key != null) col.isPK = true;
                        }
                        else if (attr_tp == typeof(System.ComponentModel.DataAnnotations.DisplayAttribute))
                        {
                            //칼럼의 디스플레이 명 뽑아내기
                            System.ComponentModel.DataAnnotations.DisplayAttribute dis = attr as System.ComponentModel.DataAnnotations.DisplayAttribute;
                            if (dis != null) col.DisplayName = dis.Name;
                        }
                        else if (attr_tp == typeof(System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute))
                        {
                            is_nomap = true;
                        }
                        
                    }

                    if (is_nomap == false) tbl.Columns.Add(col);
                }

            }
            EntityRepos.Set<T>(tbl, owner, table);
            //SchemaStore.Add(schema_key, tbl);
            //Log.Info("Add Schema : " + schema_key);
            return tbl;

        }
        */
        public static Entity EntityToDataTable<T>(T source, string owner = null, string table = null)
        {
            var t = typeof(T);

            string tablename = string.IsNullOrEmpty(table) ? t.Name : table;
            if (owner != null) tablename = owner + "." + tablename;

            Entity tbl = new Entity();
            tbl.Owner = owner;
            tbl.Name = tablename;

            var props = t.GetProperties();

            if (props != null)
            {
                int idx = 0;
                foreach (var item in props)
                {

                    bool is_nomap = false;
                    EntityProperty col = new EntityProperty();
                    col.Name = item.Name;
                    col.DisplayName = item.Name;
                    col.Index = idx;
                    idx += 1;
                    
                    col.Value = item.GetValue(source);


                    //어트리뷰트 특성값 읽어 내기
                    object[] attrs = item.GetCustomAttributes(true);

                    foreach (var attr in attrs)
                    {
                        var attr_tp = attr.GetType();

                        if (attr_tp == typeof(System.ComponentModel.DataAnnotations.KeyAttribute))
                        {
                            //기본키 뽑아내기
                            System.ComponentModel.DataAnnotations.KeyAttribute key = attr as System.ComponentModel.DataAnnotations.KeyAttribute;
                            if (key != null) col.isPK = true;
                        }
                        else if (attr_tp == typeof(System.ComponentModel.DataAnnotations.DisplayAttribute))
                        {
                            //칼럼의 디스플레이 명 뽑아내기
                            System.ComponentModel.DataAnnotations.DisplayAttribute dis = attr as System.ComponentModel.DataAnnotations.DisplayAttribute;
                            if (dis != null) col.DisplayName = dis.Name;
                        }
                        else if (attr_tp == typeof(System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute))
                        {
                            is_nomap = true;
                        }

                    }

                    if (is_nomap == false) tbl.Columns.Add(col);
                }

            }
            
            //SchemaStore.Add(schema_key, tbl);
            //Log.Info("Add Schema : " + schema_key);
            return tbl;
        }

        public static Dictionary<string, object> EntityIdToPara<T>(T source, string owner=null, string table=null)
        {
            Dictionary<string, object> paras = new Dictionary<string, object>();
            var tbl= EntityToDataTable<T>(source, owner, table);
            foreach(var col in tbl.Columns)
            {
                if (col.isPK == true)
                {
                    paras.Add(col.Name, col.Value);
                }
            }
            return paras;
        }

        public static Dictionary<string, object> EntityToPara<T>(T source, string owner = null, string table = null)
        {
            Dictionary<string, object> paras = new Dictionary<string, object>();
            var tbl = EntityToDataTable<T>(source, owner, table);
            foreach (var col in tbl.Columns)
            {
                paras.Add(col.Name, col.Value);
            }
            return paras;
        }
        public static  Dictionary<string, object> DataTablePKToPara(Entity table)
        {
            Dictionary<string, object> paras = new Dictionary<string, object>();
            foreach (var col in table.Columns)
            {
                if (col.isPK == true)
                {
                    paras.Add(col.Name, col.Value);
                }
            }
            return paras;
        }
        public static Dictionary<string, object> DataTableToPara(Entity table, string[] includeColumns = null)
        {
            Dictionary<string, object> paras = new Dictionary<string, object>();
            foreach (var col in table.Columns)
            {
                if (includeColumns != null)
                {
                    bool _isin = false;
                    foreach (var _arr_item in includeColumns)
                    {
                        if (_arr_item.ToLower() == col.Name.ToLower())
                        {
                            _isin = true;
                            break;
                        }
                    }

                    if (_isin == true)
                    {
                        paras.Add(col.Name, col.Value);
                    }
                }
                else
                {
                    paras.Add(col.Name, col.Value);
                }
            }
            return paras;
        }
    }
}
