using KafkaExchanger.Datas;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class TryProduceResult
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Exchange exchange
            )
        {
            builder.Append($@"
        public class TryProduceResult
        {{
            public bool Succsess;
            public {exchange.TypeSymbol.Name}.Response Response;
        }}
");
        }
    }
}