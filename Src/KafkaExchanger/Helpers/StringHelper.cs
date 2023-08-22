using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Helpers
{
    internal static class StringHelper
    {
        public static string ToCamel(this string str)
        {
            return char.ToLowerInvariant(str[0]) + str.Substring(1);
        }

        public static string ToPrivate(this string str)
        {
            return $"_{str}";
        }
    }
}