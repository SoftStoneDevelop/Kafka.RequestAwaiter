﻿using KafkaExchanger.Datas;
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
        public class {TypeName()}
        {{
            public bool Succsess;
            public {exchange.TypeSymbol.Name}.Response Response;
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "TryProduceResult";
        }
    }
}