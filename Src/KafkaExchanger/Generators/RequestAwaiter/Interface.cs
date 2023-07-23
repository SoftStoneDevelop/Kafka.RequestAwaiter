﻿using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Enums;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Interface
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            StartInterface(builder, requestAwaiter);

            if (requestAwaiter.Data is RequestAwaiterData)
                InterfaceProduceMethod(builder, assemblyName, requestAwaiter);

            InterfaceStartMethod(builder, assemblyName, requestAwaiter);
            InterfaceStopMethod(builder);

            EndInterfaceOrClass(builder);
        }

        private static void StartInterface(
            StringBuilder builder,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
    {requestAwaiter.Data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{requestAwaiter.Data.TypeSymbol.Name}RequestAwaiter
    {{
");
        }

        private static void InterfaceProduceMethod(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public Task<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeData = requestAwaiter.OutcomeDatas[i];
                if (!outcomeData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            {outcomeData.KeyType.GetFullTypeName(true, true)} key{i},
");
                }

                builder.Append($@"
            {outcomeData.ValueType.GetFullTypeName(true, true)} value{i},
");
            }

            builder.Append($@"
            int waitResponseTimeout = 0
            );
");
        }

        private static void InterfaceStartMethod(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public void Start(
            {requestAwaiter.Data.TypeSymbol.Name}.Config config
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@",
            {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            builder.Append($@"
            )
            ;
");
        }

        private static void InterfaceStopMethod(
            StringBuilder builder
            )
        {
            builder.Append($@"
        public Task StopAsync();
");
        }

        private static void EndInterfaceOrClass(
            StringBuilder builder
            )
        {
            builder.Append($@"
    }}
");
        }
    }
}