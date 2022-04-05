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
    public class Dao : IDisposable
    {
        static ILog Log;

        System.Data.Common.DbProviderFactory _Factory;
        System.Data.IDbConnection _Connection;
        System.Data.IDbTransaction _Tranaction;
        string _DefaultSchema = null;
        string _DBConfig_Key = "Db";
        System.Configuration.ConnectionStringSettings _DBConfig;
        string _ConnectionString = null;
        string _DBProvider = QueryHelper.DefaultDBProvider;

        /// <summary>
        /// .Net Framework 에서만 동작
        /// </summary>
        /// <param name="ConfigKey">System.Configuration.ConfigurationManager.ConnectionStrings 의 키값</param>
        /// <param name="defaultSchema">기본 Schema 또는 Owner</param>
        public Dao(string ConfigKey = null, string defaultSchema = null)
        {
            Log = Logger.GetLogger(this);
            if (string.IsNullOrEmpty(ConfigKey) == false)
            {
                DBConfig_Key = ConfigKey;
            }
            if (string.IsNullOrEmpty(defaultSchema) == false)
            {
                _DefaultSchema = defaultSchema;
            }
            //_DBConfig = System.Configuration.ConfigurationManager.ConnectionStrings[_DBConfig_Key];
            Log.Debug(string.Format("DB설정키 : {0} \n기본스키마 : {1}\nDBProvider : {2}\nDB연결문자 : {3}", DBConfig, DefaultSchema, DBProvider, ConnectionString));
        }

        /// <summary>
        /// 모든 프레임웍에서 동작
        /// </summary>
        /// <param name="fac">팩토리</param>
        /// <param name="connctionString">연결문자</param>
        /// <param name="defaultSchema">기본 Schema 또는 Owner</param>
        public Dao(System.Data.Common.DbProviderFactory fac, string connctionString, string defaultSchema = null)
        {

            Log = Logger.GetLogger(this);
            _Factory = fac;
            _DBProvider = GetDbProvider(_Factory);
            _ConnectionString = connctionString;
            if (defaultSchema != null) _DefaultSchema = defaultSchema;
        }
        /// <summary>
        /// 모든 프레임웍에서 동작
        /// </summary>
        /// <param name="con">연결</param>
        /// <param name="defaultSchema">기본 Schema 또는 Owner</param>
        public Dao(System.Data.Common.DbConnection con, string defaultSchema = null)
        {

            Log = Logger.GetLogger(this);
            _Connection = con;
            _DBProvider = GetDbProvider(con);
            _ConnectionString = con.ConnectionString;
            if (defaultSchema != null) _DefaultSchema = defaultSchema;
        }
        public Dao(System.Data.IDbConnection con, string defaultSchema = null)
        {
            Log = Logger.GetLogger(this);
            _Connection = con;
            _DBProvider = GetDbProvider(con);
            _ConnectionString = con.ConnectionString;
            if (defaultSchema != null) _DefaultSchema = defaultSchema;
        }
        private string GetDbProvider(System.Data.Common.DbProviderFactory fac)
        {
            var tname = fac.GetType().FullName;
            int idx = tname.LastIndexOf('.');
            if (idx < 1) return tname;
            return tname.Substring(0, idx);
        }
        private string GetDbProvider(System.Data.Common.DbConnection con)
        {
            var tname = con.GetType().FullName;
            int idx = tname.LastIndexOf('.');
            if (idx < 1) return tname;
            return tname.Substring(0, idx);
        }
        private string GetDbProvider(System.Data.IDbConnection con)
        {
            var tname = con.GetType().FullName;
            int idx = tname.LastIndexOf('.');
            if (idx < 1) return tname;
            return tname.Substring(0, idx);
        }
        #region property
        public string DefaultSchema
        {
            get { return _DefaultSchema; }
            set { _DefaultSchema = value; }
        }

        System.Configuration.ConnectionStringSettings DBConfig
        {
            get
            {
                if (_DBConfig == null)
                {
                    _DBConfig = System.Configuration.ConfigurationManager.ConnectionStrings[_DBConfig_Key];
                    _ConnectionString = _DBConfig.ConnectionString;
                    _DBProvider = _DBConfig.ProviderName;
                }
                return _DBConfig;
            }
        }

        public string ConnectionString
        {
            get
            {
                return _ConnectionString;
            }
            set
            {
                _ConnectionString = value;
            }
        }
        public string DBProvider
        {
            get
            {
                return _DBProvider;
            }
            set
            {
                _DBProvider = value;
            }
        }
        public string DBConfig_Key
        {
            get
            {
                return _DBConfig_Key;
            }
            set
            {
                _DBConfig_Key = value;
                _DBConfig = System.Configuration.ConfigurationManager.ConnectionStrings[_DBConfig_Key];
                _ConnectionString = _DBConfig.ConnectionString;
                _DBProvider = _DBConfig.ProviderName;
            }
        }

        public string GetOwner(string Owner = null)
        {
            return string.IsNullOrEmpty(Owner) ? DefaultSchema : Owner;
        }
        #endregion

        #region connection
        public IDbConnection Connection
        {
            get
            {
                if (_Connection == null)
                {
                    _Connection = CreateConnection();
                }
                return _Connection;
            }
            set
            {
                _Connection = value;
            }
        }
        private IDbConnection CreateConnection()
        {
            IDbConnection con;
            if (_Factory == null)
            {
                _Factory = System.Data.Common.DbProviderFactories.GetFactory(DBProvider);
            }
            
            //var fac = DbProviderFactories.GetFactory(DBProvider);
            con = _Factory.CreateConnection();
            con.ConnectionString = ConnectionString;
            return con;
        }
        #endregion

        #region trasaction
        public IDbTransaction BeginTransaction()
        {
            if (Connection.State == ConnectionState.Closed || Connection.State == ConnectionState.Broken)
            {
                Connection.Open();
            }
            _Tranaction = Connection.BeginTransaction();
            return _Tranaction;
        }
        public void CommitTranction()
        {
            if (this._Tranaction != null)
            {
                this._Tranaction.Commit();
                this._Tranaction.Dispose();
            }
        }
        public void RollbackTranction()
        {
            if (this._Tranaction != null)
            {
                this._Tranaction.Rollback();
                this._Tranaction.Dispose();
            }
        }
        #endregion


        #region dispose
        public void Dispose()
        {
            Connection.Dispose();
            //GC.SuppressFinalize(this);
        }
        #endregion

        public EntityDao<T> GetEntityDao<T>(string Owner=null, string Table=null)
        {
            return new EntityDao<T>(Connection, DBProvider, GetOwner(Owner), Table);
        }

        #region select 
        public List<T> Select<T>(Dictionary<string, object> WherePara = null, string WhereSql = null, string Orders = null,  string[] SelectColumns = null, string Owner = null, string Table=null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);

            return Tdao.Select(WherePara, WhereSql, Orders, SelectColumns);
            /*
            var query = QueryHelper.Sql_Select<T>(SelectColumns, WherePara, WhereSql, Orders, owner, Table,  DBProvider);

            Logger.WriteQuery(Log, query);
            

            if (WherePara != null)
            {
                if (WherePara.Count > 0)
                {
                    return Connection.Query<T>(query.Item1, query.Item2).ToList();
                }
            }
            return Connection.Query<T>(query.Item1).ToList();
            */
        }
        public T SelectOne<T>(Dictionary<string, object> WherePara = null, string WhereSql = null, string[] SelectColumns = null, string Owner = null, string Table = null)
        {

            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);

            return Tdao.SelectOne(WherePara, WhereSql, SelectColumns);
            /*
            var query = QueryHelper.Sql_Select<T>(SelectColumns, WherePara, WhereSql, null, owner, Table, DBProvider);
            Logger.WriteQuery(Log, query);
            if (WherePara != null)
            {
                if (WherePara.Count > 0)
                {
                    return Connection.QuerySingle<T>(query.Item1, query.Item2);
                }
            }
            return Connection.QuerySingle<T>(query.Item1);
            */
        }

        public T SelectById<T>(T entity, string[] SelectColumns = null, string Owner=null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table); ;
            return Tdao.SelectById(entity,  SelectColumns );
        }

        public DataSet SelectDataSet<T>(Dictionary<string, object> WherePara = null, string WhereSql = null, string Orders = null, string[] SelectColumns = null, string Owner = null, string Table = null)
        {
            var query = QueryHelper.Sql_Select<T>(SelectColumns,WherePara, WhereSql, Orders, GetOwner(Owner), Table, DBProvider);
            Logger.WriteQuery(Log, query);
            return SelectDataSet(query.Item1, query.Item2);
        }
        public DataSet SelectDataSet(string Query, Dictionary<string, object> WherePara = null)
        {
            DataSet ds = new DataSet();
            var tbl = ds.Tables.Add();
            tbl.Load(Connection.ExecuteReader(Query, WherePara));
            return ds;
        }
        #endregion

        #region Insert
        public int Insert<T>(Dictionary<string, object> inPara, string Owner = null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);
            return Tdao.Insert(inPara);
            /*
            var query = QueryHelper.Sql_Insert<T>(inPara, GetOwner(Owner), Table, DBProvider);
            Logger.WriteQuery(Log, query);
            return Connection.Execute(query.Item1, query.Item2, _Tranaction);
            */
        }
        public int Insert<T>(T entity, string[] IncludeColumns = null, string Owner = null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);
            return Tdao.Insert(entity, IncludeColumns);
        }

        #endregion

        #region Update
        public int Update<T>(Dictionary<string, object> UpdatePara, Dictionary<string, object> WherePara, string WhereSql = null, string Owner = null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);
            return Tdao.Update(UpdatePara, WherePara, WhereSql);
            /*
            var query = QueryHelper.Sql_Update<T>(UpdatePara, WherePara, WhereSql, GetOwner(Owner), Table, DBProvider);
            Logger.WriteQuery(Log, query);
            return Connection.Execute(query.Item1, query.Item2, _Tranaction);
            */
        }
        public int Update<T>(T entity,  string[] IncludeColumns = null, string Owner = null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);
            return Tdao.Update(entity, IncludeColumns);
            
        }

        public int InsertOrUpdate<T>(T entity, string[] includeColumns = null, string Owner = null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);
            return Tdao.InsertOrUpdate(entity, includeColumns);
        }
        #endregion

        #region delete

        public int Delete<T>(Dictionary<string, object> WherePara, string WhereSql = null, string Owner = null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);
            return Tdao.Delete(WherePara, WhereSql);
            /*
            var query = QueryHelper.Sql_Delete<T>(WherePara, WhereSql, GetOwner(Owner), Table, DBProvider);
            Logger.WriteQuery(Log, query);
            return Connection.Execute(query.Item1, query.Item2, _Tranaction);
            */
        }
        public int DeleteById<T>(T entity, string Owner = null, string Table = null)
        {
            var Tdao = GetEntityDao<T>(GetOwner(Owner), Table);
            return Tdao.Delete(entity);
        }
        #endregion

    }
}
