using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace Dau.Data
{
    public class Logger
    {
        //public static readonly ILog Log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public static ILog GetLogger(Object o)
        {
            var tp = o.GetType();
            ILog Log = LogManager.GetLogger(tp);
            return Log;
        }
        public static void WriteQuery(ILog log, Tuple<string, Dictionary<string, object>> query)
        {
            string log_msg = "\r\n" + query.Item1 + "\r\n";
            
            if (query.Item2 != null)
            {
                int cnt = 0;
                foreach (var _key in query.Item2.Keys)
                {
                    cnt += 1;
                    log_msg += string.Format("\r\n{0}\t[{1}] : {2}", cnt, _key, query.Item2[_key]);
                }
            }
            log.Debug(log_msg);
        }
    }
}
