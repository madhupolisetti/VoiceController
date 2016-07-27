using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public static class BulkRequestId
    {
        private static Dictionary<Environment, long> ids = new Dictionary<Environment, long>();
        public static void SetLastId(long id, Environment environment)
        {
            lock (ids)
            {
                if (!ids.ContainsKey(environment))
                    ids.Add(environment, id);
                else
                    ids[environment] = id;
            }
        }
        public static long GetLastId(Environment environment)
        {
            lock (ids)
            {
                if (!ids.ContainsKey(environment))
                {
                    ids[environment] = 0;
                }
                return ids[environment];
            }
        }
    }
}
