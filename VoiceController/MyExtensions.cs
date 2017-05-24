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
        public static string ToString(this System.Data.DataRow row)
        {
            string data = string.Empty;
            foreach (System.Data.DataColumn column in row.Table.Columns)
            {
                if (row[column].IsDBNull())
                    data += string.Format("{0} : NULL", column.ColumnName);
                else
                    data += string.Format("{0} : {1}", column.ColumnName, row[column].ToString());
                data += System.Environment.NewLine;
            }
            return data;
        }
    }
}
