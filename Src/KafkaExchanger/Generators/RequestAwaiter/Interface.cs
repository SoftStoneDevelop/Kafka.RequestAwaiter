using KafkaExchanger.AttributeDatas;
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
            InterfaceProduceMethod(builder, assemblyName, requestAwaiter);
            InterfaceProduceDelayMethod(builder, assemblyName, requestAwaiter);
            InterfaceStartMethod(builder, assemblyName, requestAwaiter);
            InterfaceSetupMethod(builder, assemblyName, requestAwaiter);
            InterfaceStopMethod(builder);

            EndInterfaceOrClass(builder);
        }

        private static void StartInterface(
            StringBuilder builder,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
    {requestAwaiter.Data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{requestAwaiter.Data.TypeSymbol.Name}RequestAwaiter : IAsyncDisposable
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
        public ValueTask<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            {outputData.KeyType.GetFullTypeName(true, true)} key{i},
");
                }

                builder.Append($@"
            {outputData.ValueType.GetFullTypeName(true, true)} value{i},
");
            }

            builder.Append($@"
            int waitResponseTimeout = 0
            );
");
        }

        private static void InterfaceProduceDelayMethod(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public ValueTask<{requestAwaiter.Data.TypeSymbol.Name}.DelayProduce> ProduceDelay(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            {outputData.KeyType.GetFullTypeName(true, true)} key{i},
");
                }

                builder.Append($@"
            {outputData.ValueType.GetFullTypeName(true, true)} value{i},
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
        public void Start(Action<Confluent.Kafka.ConsumerConfig> changeConfig = null);
");
        }

        private static void InterfaceSetupMethod(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public void Setup(
            {requestAwaiter.Data.TypeSymbol.Name}.Config config
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@",
            {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
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
        public ValueTask StopAsync();
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