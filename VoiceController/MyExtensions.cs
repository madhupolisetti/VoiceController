using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public static class MyExtensions
    {
        public static bool IsDBNull(this object input)
        {
            return input.Equals(System.DBNull.Value);
        }
    }
}
