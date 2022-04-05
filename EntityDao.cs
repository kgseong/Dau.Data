using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Common;
using Dapper;
using log4net;

namespace Dau.Data
{
    public class EntityDao<T> //where T :class
    {
        static ILog Log;

        System.Data.IDbTransaction trans;
        IDbConnection Connection;
        string DBProvider;
        string Owner;
        string Table;
        
        public EntityDao(IDbConnection connection, string dbprovider,  string owner=null, string table=null)
        {
            Log = Logger.GetLogger(this);

            Connection = connection;
            DBProvider = dbprovider;

            Owner = owner;
            Table = table;


            
        }
        #region Transction
        public System.Data.IDbTransaction BeginTransaction()
        {
            if (Connection.State == ConnectionState.Closed || Connection.State == ConnectionState.Broken)
            {
                Connection.Open();
            }
            trans = Connection.BeginTransaction();
            return trans;
        }
        public void CommitTransaction()
        {
            if (trans != null)
            {
                trans.Commit();
                trans.Dispose();
            }
        }
        public void RollbackTranction()
        {
            if (trans != null)
            {
                trans.Rollback();
                trans.Dispose();
            }
        }
        #endregion

        #region Helper
        /*
        protected Dictionary<string, object> EntityIdToPara(DataTable table)
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
        protected Dictionary<string, object> EntityToPara(DataTable table, string[] includeColumns=null)
        {
            Dictionary<string, object> paras = new Dictionary<string, object>();
            foreach (var col in table.Columns)
            {
                if (includeColumns != null)
                {
                    bool _isin = false;
                    foreach(var _arr_item in includeColumns)
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
                }else 
                {
                    paras.Add(col.Name, col.Value);
                } 
            }
            return paras;
        }*/
        #endregion
        #region select
        public T SelectById(T entity, string[] SelectColumns = null)
        {
            var tbl = EntityHelper.EntityToDataTable<T>(entity, Owner, Table);
            tbl.Owner = Owner;
            var idPara = EntityHelper.DataTablePKToPara(tbl);
            if (idPara == null || idPara.Count < 1) throw new Exception("There is No Primary key");
            var query = QueryHelper.Sql_Select<T>(SelectColumns, idPara, null, null, tbl.Owner, tbl.TableName, DBProvider);
            Logger.WriteQuery(Log, query);
            var items = Connection.Query<T>(query.Item1, query.Item2);
            //if (items == null) return default(T);
            //if (items.Count() < 1) return default(T);
            foreach(var item in items)
            {
                return item;
            }
            return default(T);
        }

        public T SelectOne(Dictionary<string, object> WherePara = null, string WhereClause = null, string[] SelectColumns=null)
        {
            var query = QueryHelper.Sql_Select<T>(SelectColumns, WherePara, WhereClause,null, Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            if (WherePara != null)
            {
                if (WherePara.Count > 0)
                {
                    return Connection.QuerySingle<T>(query.Item1, query.Item2);
                }
            }
            return Connection.QuerySingle<T>(query.Item1);
        }

        public List<T> Select(Dictionary<string, object> WherePara = null, string WhereClause=null, string OrderClause=null, string[] SelectColumns = null)  
        {    
            var query = QueryHelper.Sql_Select<T>(SelectColumns, WherePara, WhereClause, OrderClause, Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            if (WherePara != null)
            {
                if (WherePara.Count > 0)
                {
                    return Connection.Query<T>(query.Item1, query.Item2).ToList();
                }
            }
            return Connection.Query<T>(query.Item1).ToList();
        }

        public List<T> SelectPaged(Dictionary<string, object> WherePara = null, string WhereClause = null, string OrderClause = null, string[] SelectColumns = null, int page=1, int page_size=20)
        {
            List<T> result = new List<T>();
            List<T> list = Select(WherePara, WhereClause, OrderClause, SelectColumns);
            if (list == null) return null;
            /*
            int s_po= (page - 1) * page_size;
            int e_po = s_po + page_size;
            if (s_po < 0) s_po = 0;
            if (e_po > list.Count) e_po = list.Count;

            for(int idx = 0; idx < list.Count; idx ++)
            {
                if (idx >= s_po && idx < e_po)
                {
                    result.Add(list[idx]);
                }
            }
            
            return result;
            */
            return list.Skip(page * page_size).Take(page_size).ToList();
        }
        #endregion


        #region Insert
        public int Insert(T entity, string[] includeColumns=null)
        {
            var tbl = EntityHelper.EntityToDataTable<T>(entity, Owner, Table);
            tbl.Owner = Owner;
            
            var entityPara = EntityHelper.DataTableToPara(tbl, includeColumns);

            var query = QueryHelper.Sql_Insert<T>(entityPara, tbl.Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            return Connection.Execute(query.Item1, query.Item2, trans);
        }
        public int Insert(Dictionary<string, object> Para)
        {
            var query = QueryHelper.Sql_Insert<T>(Para, Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            return Connection.Execute(query.Item1, query.Item2, trans);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <returns>1:inserted, 0:updated, -1:noaffect</returns>
        public int InsertOrUpdate(T entity, string[] includeColumns = null)
        {
            try
            {
                var cnt = Update(entity, includeColumns);
                if (cnt <1)
                {
                    cnt = Insert(entity, includeColumns);
                    return 1;
                }else
                {
                    return 0;
                }
            }
            catch
            {
            }
            return -1;
        }
        #endregion

        #region Update
        public int Update(T entity, string[] includeColumns = null)
        {
            var tbl = EntityHelper.EntityToDataTable<T>(entity, Owner, Table);
            tbl.Owner = Owner;
            var idPara = EntityHelper.DataTablePKToPara(tbl);
            if (idPara == null || idPara.Count < 1) throw new Exception("There is No Primary key");

            var entityPara = EntityHelper.DataTableToPara(tbl, includeColumns);

            var query = QueryHelper.Sql_Update<T>(entityPara, idPara, null, tbl.Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            if (idPara != null)
            {
                if (idPara.Count > 0)
                {
                    return Connection.Execute(query.Item1, query.Item2, trans);
                }
            }
            return Connection.Execute(query.Item1,null, trans);
        }

        public int Update(T entity, Dictionary<string, object> WherePara, string WhereSql = null, string[] includeColumns = null)
        {
            var tbl = EntityHelper.EntityToDataTable<T>(entity, Owner, Table);
            tbl.Owner = Owner;
            
            var entityPara = EntityHelper.DataTableToPara(tbl, includeColumns);

            var query = QueryHelper.Sql_Update<T>(entityPara, WherePara, WhereSql, tbl.Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            return Connection.Execute(query.Item1, null, trans);
        }
        
        public int Update(Dictionary<string, object> UpdatePara = null, Dictionary<string, object> WherePara = null, string WhereClause = null)
        {
            var query = QueryHelper.Sql_Update<T>(UpdatePara, WherePara, WhereClause, Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            if (WherePara != null)
            {
                if (WherePara.Count > 0)
                {
                    return Connection.Execute(query.Item1, query.Item2, trans);
                }
            }
            return Connection.Execute(query.Item1, null, trans);
        }

        #endregion

        #region Delete
        public int Delete(T entity)
        {
            var tbl = EntityHelper.EntityToDataTable<T>(entity, Owner, Table);
            tbl.Owner = Owner;
            var idPara = EntityHelper.DataTablePKToPara(tbl);
            if (idPara == null || idPara.Count < 1) throw new Exception("There is No Primary key");

            var entityPara = EntityHelper.DataTableToPara(tbl, null);

            var query = QueryHelper.Sql_Delete<T>(idPara, null, tbl.Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            if (idPara != null)
            {
                if (idPara.Count > 0)
                {
                    return Connection.Execute(query.Item1, query.Item2, trans);
                }
            }
            return Connection.Execute(query.Item1,null, trans);
        }
        public int Delete(Dictionary<string, object> WherePara = null, string WhereClause = null)
        {
            var query = QueryHelper.Sql_Delete<T>(WherePara, WhereClause, Owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            if (WherePara != null)
            {
                if (WherePara.Count > 0)
                {
                    return Connection.Execute(query.Item1, query.Item2, trans);
                }
            }
            return Connection.Execute(query.Item1, null, trans);
        }
        #endregion

    }
}
