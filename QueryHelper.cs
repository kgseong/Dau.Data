using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dau.Data
{
    public class QueryHelper
    {
        public const string DefaultDBProvider = "System.Data.SqlClient";
        static Dictionary<string, char> _SqlParameterPrefix;

        public static Dictionary<string, char> SqlParameterPrefix
        {
            get
            {
                if (_SqlParameterPrefix == null)
                {
                    _SqlParameterPrefix = new Dictionary<string, char>();
                    _SqlParameterPrefix.Add("SQL", '@');
                    _SqlParameterPrefix.Add("System.Data.SqlClient", '@');
                    _SqlParameterPrefix.Add("Oracle", ':');
                    _SqlParameterPrefix.Add("Oracle.DataAccess.Client", ':');
                    _SqlParameterPrefix.Add("Oracle.DataAccess", ':');
                    _SqlParameterPrefix.Add("Oracle.DataAccess.Lite_w32", ':');
                    _SqlParameterPrefix.Add("System.Data.OleDb", '\0');
                    _SqlParameterPrefix.Add("System.Data.Odbc", '\0');
                    _SqlParameterPrefix.Add("System.Data.OracleClient", ':');
                    _SqlParameterPrefix.Add("System.Data.SqlServerCe", '@');
                    _SqlParameterPrefix.Add("MySql", '?');
                    _SqlParameterPrefix.Add("MySql.Data.MySqlClient", '?');
                    _SqlParameterPrefix.Add("Devart.Data.MySql", '@');
                    _SqlParameterPrefix.Add("SQLite", '@');
                    _SqlParameterPrefix.Add("System.Data.SQLite", '@');
                    _SqlParameterPrefix.Add("DB2", '?');
                    _SqlParameterPrefix.Add("IBM.Data.DB2", '?');
                    _SqlParameterPrefix.Add("Community.CsharpSqlite.SQLiteClient", '@');
                    _SqlParameterPrefix.Add("FirebirdSql.Data.FirebirdClient", '@');
                    _SqlParameterPrefix.Add("Sap.Data.Hana", '\0');
                    _SqlParameterPrefix.Add("Ingres.Client", '\0');
                    _SqlParameterPrefix.Add("Npgsql", ':');
                    _SqlParameterPrefix.Add("iAnywhere.Data.AsaClient", '\0');
                    _SqlParameterPrefix.Add("Sybase.AdoNet2.AseClient", '@');
                    _SqlParameterPrefix.Add("iAnywhere.Data.SQLAnywhere", ':');
                }
                return _SqlParameterPrefix;
            }
        }


        public static Tuple<string, Dictionary<string, object>> Sql_Select<T>(string[] SelectColumns = null, Dictionary<string, object> WherePara = null, string WhereSql = null, string Orders = "", string Owner = null, string Table=null,  string DBProvider = DefaultDBProvider)
        {
            StringBuilder sb_query = new StringBuilder("SELECT ");
            if (SelectColumns == null)
            {
                sb_query.Append(" * ");
            }else
            {
                sb_query.Append(string.Join(",", SelectColumns));
            }
            sb_query.Append(" FROM ");
            sb_query.Append(GetTableName<T>(Owner, Table));
            var p_where = ConvertWhere(WherePara, WhereSql, DBProvider);
            sb_query.Append(p_where.Item1);
            if (string.IsNullOrEmpty(Orders) == false)
            {
                sb_query.Append(" ORDER BY ");
                sb_query.Append(Orders);
            }

            return new Tuple<string, Dictionary<string, object>>(sb_query.ToString(), p_where.Item2);
        }

        public static Tuple<string, Dictionary<string, object>> Sql_Update<T>(Dictionary<string, object> UpdatePara, Dictionary<string, object> WherePara, string WhereSql = null, string Owner = null, string Table = null, string DBProvider = DefaultDBProvider)
        {

            StringBuilder sb_query = new StringBuilder("UPDATE ");
            sb_query.Append(GetTableName<T>(Owner, Table));
            sb_query.Append(" SET ");


            if (UpdatePara != null)
            {
                int seq = 0;
                foreach (var key in UpdatePara.Keys)
                {
                    if (seq != 0)
                    {
                        sb_query.Append(" , ");
                    }
                    sb_query.Append(key);
                    sb_query.Append("=");
                    sb_query.Append(SqlParameterPrefix[DBProvider]);
                    sb_query.Append(key);

                    seq += 1;
                }
            }
            var p_where = ConvertWhere(WherePara, WhereSql, DBProvider);
            sb_query.Append(p_where.Item1);

            Dictionary<string, object> return_para = new Dictionary<string, object>();
            foreach (var item in UpdatePara.Keys)
            {
                return_para.Add(item, UpdatePara[item]);
            }
            foreach (var item in p_where.Item2.Keys)
            {
                return_para.Add(item, p_where.Item2[item]);
            }

            return new Tuple<string, Dictionary<string, object>>(sb_query.ToString(), return_para);
        }

        public static Tuple<string, Dictionary<string, object>> Sql_Insert<T>(Dictionary<string, object> InsertPara, string Owner = null, string Table = null, string DBProvider = DefaultDBProvider)
        {
            StringBuilder sb_query = new StringBuilder("INSERT INTO ");
            StringBuilder sb_col = new StringBuilder();
            StringBuilder sb_para = new StringBuilder();

            sb_query.Append(GetTableName<T>(Owner, Table));

            if (InsertPara != null)
            {
                int seq = 0;
                foreach (var key in InsertPara.Keys)
                {
                    if (seq != 0)
                    {
                        sb_col.Append(" , ");
                        sb_para.Append(" , ");
                    }

                    sb_col.Append(key);
                    sb_para.Append(SqlParameterPrefix[DBProvider]);
                    sb_para.Append(key);

                    seq += 1;
                }
            }
            sb_query.Append("(");
            sb_query.Append(sb_col.ToString());
            sb_query.Append(") values (");
            sb_query.Append(sb_para.ToString());
            sb_query.Append(")");

            return new Tuple<string, Dictionary<string, object>>(sb_query.ToString(), InsertPara);
        }

        public static Tuple<string, Dictionary<string, object>> Sql_Delete<T>(Dictionary<string, object> WherePara, string WhereSql = null, string Owner = null, string Table = null, string DBProvider = DefaultDBProvider)
        {
            StringBuilder sb_query = new StringBuilder("DELETE FROM  ");
            sb_query.Append(GetTableName<T>(Owner, Table));
            var p_where = ConvertWhere(WherePara, WhereSql, DBProvider);
            sb_query.Append(p_where.Item1);
            return new Tuple<string, Dictionary<string, object>>(sb_query.ToString(), p_where.Item2);
        }

        #region obj to dic
        /*
        public static Dictionary<string, object> ToDictionary<T>(T source, bool isExcludeNull = true)
        {
            IEnumerable<PropertyInfo> props;
            Dictionary<string, object> dic;
            if (isExcludeNull == true)
            {
                props = source.GetType().GetProperties()
                 .Where(p => p.GetValue(source) != null);
            }
            else
            {
                props = source.GetType().GetProperties()
                 .Where(p => p.GetValue(source) != null);
            }
            if (props != null)
            {
                dic = new Dictionary<string, object>();
                foreach (var item in props)
                {
                    dic.Add(item.Name, item.GetValue(source));
                }
                return dic;
            }
            return null;
        }
        */
        #endregion

        #region convert query 
            
        private static string GetTableName<T>(string Owner = null, string Table=null)
        {

            var _t = typeof(T);
            string _schema = string.IsNullOrEmpty(Owner) ? "" : Owner;
            string _tbl = string.IsNullOrEmpty(Table) ? _t.Name : Table;
            string tbl = string.IsNullOrEmpty(_schema) == false ? _schema + "." + _tbl : _tbl;
            return tbl;
        }
        private static Tuple<string, Dictionary<string, object>> ConvertWhere(Dictionary<string, object> WherePara = null, string WhereSql = null, string DBProvider = DefaultDBProvider)
        {
            if (WherePara == null && WhereSql == null)
            {
                return new Tuple<string, Dictionary<string, object>>("", null);
            }
            Dictionary<string, object> _WherePara = new Dictionary<string, object>();
            StringBuilder sb_where = new StringBuilder(" where ");

            if (string.IsNullOrEmpty(WhereSql) == false)
            {
                sb_where.Append(ConvertSql(WhereSql, DBProvider,  true));
                if (WherePara != null)
                {
                    foreach (var key in WherePara.Keys)
                    {
                        _WherePara.Add("p_" + key, WherePara[key]);
                    }
                }
            }
            else
            {
                int seq = 0;
                if (WherePara != null)
                {
                    foreach (var key in WherePara.Keys)
                    {
                        if (seq != 0)
                        {
                            sb_where.Append(" and ");
                        }
                        sb_where.Append(key);
                        sb_where.Append("=");
                        sb_where.Append(SqlParameterPrefix[DBProvider]);
                        sb_where.Append("p_");
                        sb_where.Append(key);

                        _WherePara.Add("p_" + key, WherePara[key]);
                        seq += 1;
                    }
                }
            }
            return new Tuple<string, Dictionary<string, object>>(sb_where.ToString(), _WherePara);
        }

        public static string ConvertSql(string Query, string DBProvider = "System.Data.SqlClient", bool isWhereClause=false)
        {
            char[] prefixs = new char[] { ':', '@', '?' };
            //var qs = Query.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var cs = Query.ToCharArray();
            StringBuilder sb = new StringBuilder();
            int q_cnt = 0;
            foreach (var c in cs)
            {
                if (c == '\'') q_cnt += 1;
                if (q_cnt % 2 == 1)
                {
                    sb.Append(c);
                    continue;
                }
                bool is_prefix = false;
                foreach (var prefix in prefixs)
                {
                    if (c == prefix)
                    {
                        is_prefix = true;
                        break;
                    }
                }
                if (is_prefix == true)
                {
                    sb.Append(SqlParameterPrefix[DBProvider]);
                    if (isWhereClause==true)
                    {
                        sb.Append("p_");
                    }
                }
                else
                {
                    sb.Append(c);
                }
            }

            return sb.ToString();

            //"GetParameterName"
        }
        /*
        public static string ConvertSqlWhere(string Query, string DBProvider = "Oracle.DataAccess.Client")
        {
            char[] prefixs = new char[] { ':', '@', '?' };
            //var qs = Query.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            var cs = Query.ToCharArray();
            StringBuilder sb = new StringBuilder();
            int q_cnt = 0;

            foreach (var c in cs)
            {
                if (c == '\'') q_cnt += 1;
                if (q_cnt % 2 == 1)
                {
                    sb.Append(c);
                    continue;
                }
                bool is_prefix = false;
                foreach (var prefix in prefixs)
                {
                    if (c == prefix)
                    {
                        is_prefix = true;
                        break;
                    }
                }
                if (is_prefix == true)
                {
                    sb.Append(SqlParameterPrefix[DBProvider]);
                    sb.Append("p_");
                }
                else
                {
                    sb.Append(c);
                }
            }

            return sb.ToString();
        }
        */
        #endregion
    }
}
