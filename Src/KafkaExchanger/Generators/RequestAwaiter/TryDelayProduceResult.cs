﻿using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class TryDelayProduceResult
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            public bool Succsess;
            public {requestAwaiter.TypeSymbol.Name}.TopicResponse Response;
            public {requestAwaiter.TypeSymbol.Name}.PartitionItem.Bucket Bucket;
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public {assemblyName}.RequestHeader {outputData.NamePascalCase}Header;
            public {outputData.MessageTypeName} {outputData.MessageTypeName};
");
            }

            builder.Append($@"
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "TryDelayProduceResult";
        }
    }
}