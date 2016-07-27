using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public static class CustomEntensions
    {
        public static string UrlEncode(this string input)
        {
            return System.Web.HttpUtility.UrlEncode(input);
        }
    }
}
