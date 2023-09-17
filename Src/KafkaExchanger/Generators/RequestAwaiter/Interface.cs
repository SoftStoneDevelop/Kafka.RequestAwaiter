using KafkaExchanger.Datas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Interface
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            StartInterface(builder, requestAwaiter);
            InterfaceProduceMethod(builder, requestAwaiter);
            InterfaceProduceDelayMethod(builder, assemblyName, requestAwaiter);
            InterfaceStartMethod(builder, assemblyName, requestAwaiter);
            InterfaceSetupMethod(builder, assemblyName, requestAwaiter);
            AddAwaiter(builder, assemblyName, requestAwaiter);
            InterfaceStopMethod(builder);

            EndInterfaceOrClass(builder);
        }

        private static void StartInterface(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
    {requestAwaiter.TypeSymbol.DeclaredAccessibility.ToName()} partial interface I{requestAwaiter.TypeSymbol.Name}RequestAwaiter : IAsyncDisposable
    {{");
        }

        private static void InterfaceProduceMethod(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            string outputKeyParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Key";
            }

            string outputValueParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Value";
            }

            builder.Append($@"
        public Task<{requestAwaiter.TypeSymbol.Name}.Response> Produce(");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            {outputData.KeyType.GetFullTypeName(true, true)} {outputKeyParam(outputData)},");

                }

                builder.Append($@"
            {outputData.ValueType.GetFullTypeName(true, true)} {outputValueParam(outputData)},");

            }

            builder.Append($@"
            int waitResponseTimeout = 0
            );
");
        }

        private static void InterfaceProduceDelayMethod(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            string outputKeyParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Key";
            }

            string outputValueParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Value";
            }

            builder.Append($@"
        public Task<{DelayProduce.TypeFullName(requestAwaiter)}> ProduceDelay(");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            {outputData.KeyType.GetFullTypeName(true, true)} {outputKeyParam(outputData)},");

                }

                builder.Append($@"
            {outputData.ValueType.GetFullTypeName(true, true)} {outputValueParam(outputData)},");

            }

            builder.Append($@"
            int waitResponseTimeout = 0
            );
");
        }

        private static void InterfaceStartMethod(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public void Start(Action<Confluent.Kafka.ConsumerConfig> changeConfig = null);");
        }

        private static void InterfaceSetupMethod(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public Task Setup(
            {Config.TypeFullName(requestAwaiter)} config");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@",
            KafkaExchanger.IProducerPool<{requestAwaiter.OutputDatas[i].TypesPair}> producerPool{i}");

            }
            builder.Append($@",
            {requestAwaiter.BucketsCountFuncType()} currentBucketsCount
            )
            ;
");
        }

        private static void AddAwaiter(
            StringBuilder builder, 
            string assemblyName, 
            Datas.RequestAwaiter requestAwaiter
            )
        {
            string partitionsParam(InputData inputData)
            {
                return $@"{inputData.NameCamelCase}Partitions";
            }

            builder.Append($@"
        public Task<{Response.TypeFullName(requestAwaiter)}> AddAwaiter(
            string messageGuid,
            int bucket,");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            int[] {partitionsParam(inputData)},");
            }
            builder.Append($@"
            int waitResponseTimeout = 0
            );
");
        }

        private static void InterfaceStopMethod(
            StringBuilder builder
            )
        {
            builder.Append($@"
        public Task StopAsync(CancellationToken token = default);");
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