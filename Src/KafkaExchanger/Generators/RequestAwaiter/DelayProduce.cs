using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class DelayProduce
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            StartClass(builder, requestAwaiter);
            Fields(builder, assemblyName, requestAwaiter);
            Produce(builder, requestAwaiter);
            Dispose(builder, assemblyName);
            EndClass(builder);
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "DelayProduce";
        }

        public static string Bucket()
        {
            return "Bucket";
        }

        private static string _tryDelay()
        {
            return "_tryDelay";
        }

        public static string Partitions(InputData inputData)
        {
            return $"{inputData.NamePascalCase}Partitions";
        }

        public static string Header(OutputData outputData)
        {
            return $"{outputData.NamePascalCase}Header";
        }

        public static string Message(OutputData outputData)
        {
            return $"{outputData.MessageTypeName}";
        }

        private static void StartClass(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()} : IDisposable
        {{
            private {TypeName()}(){{}}
            public {TypeName()}(
                {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult tryDelay)
            {{
                {_tryDelay()} = tryDelay;
            }}
");
        }

        private static void EndClass(
            StringBuilder builder
            )
        {
            builder.Append($@"
        }}
");
        }

        private static void Fields(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult {_tryDelay()};
            public int {Bucket()} => {_tryDelay()}.{TryDelayProduceResult.Bucket()}.{KafkaExchanger.Generators.RequestAwaiter.Bucket.BucketId()};
            public string MessageGuid => {_tryDelay()}.{TryDelayProduceResult.Response()}.{TopicResponse.MessageGuid()};
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            public int[] {Partitions(inputData)} => {_tryDelay()}.{TryDelayProduceResult.Bucket()}.{KafkaExchanger.Generators.RequestAwaiter.Bucket.Partitions(inputData)};
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public {assemblyName}.RequestHeader {Header(outputData)} => {_tryDelay()}.{TryDelayProduceResult.Header(outputData)};
            public {outputData.MessageTypeName} {Message(outputData)} => {_tryDelay()}.{TryDelayProduceResult.Message(outputData)};
");
            }
        }

        private static void Produce(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public ValueTask<{requestAwaiter.TypeSymbol.Name}.Response> Produce()
            {{
                if(_produced)
                {{
                    throw new System.Exception(""Produce can not be called twice"");
                }}

                _produced = true;
                return 
                    {_tryDelay()}.{TryDelayProduceResult.Bucket()}.Produce({_tryDelay()});
            }}
");
        }

        private static void Dispose(
            StringBuilder builder,
            string assemblyName
            )
        {
            builder.Append($@"
            private bool _disposedValue;
            private bool _produced;

            public void Dispose()
            {{
                Dispose(true);
                GC.SuppressFinalize(this);
            }}

            protected void Dispose(bool disposed)
            {{
                if (!_disposedValue)
                {{
                    if (!_produced)
                    {{
                        {_tryDelay()}.{TryDelayProduceResult.Bucket()}.RemoveAwaiter({_tryDelay()}.{TryDelayProduceResult.Response()}.{TopicResponse.MessageGuid()});
                    }}

                    {_tryDelay()}.{TryDelayProduceResult.Bucket()} = null;
                    {_tryDelay()} = null;
                    _disposedValue = true;
                }}
            }}

            ~DelayProduce()
            {{
                Dispose(false);
            }}
");
        }
    }
}