using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class DelayProduce
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.GenerateData requestAwaiter
            )
        {
            StartClass(builder, requestAwaiter);
            Fields(builder, requestAwaiter);
            Produce(builder, assemblyName);
            Dispose(builder, assemblyName);
            EndClass(builder);
        }

        private static void StartClass(
            StringBuilder builder,
            AttributeDatas.GenerateData requestAwaiter
            )
        {
            builder.Append($@"
        public class DelayProduce : IDisposable
        {{
            private DelayProduce(){{}}
            public DelayProduce(
                {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult tryDelay)
            {{
                _tryDelay = tryDelay;
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
            AttributeDatas.GenerateData requestAwaiter
            )
        {
            builder.Append($@"
            private {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult _tryDelay;
            public int Bucket => _tryDelay.Bucket.BucketId;
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas;
                builder.Append($@"
            public int[] InputTopic{i}Partitions => _tryDelay.Bucket.InputTopic{i}Partitions;
");
            }
        }

        private static void Produce(
            StringBuilder builder,
            string assemblyName
            )
        {
            builder.Append($@"
            public async Task<{assemblyName}.Response> Produce()
            {{
                if(_produced)
                {{
                    throw new System.Exception(""Produce can not be called twice"");
                }}

                _produced = true;
                return 
                    await _tryDelay.Bucket.Produce(_tryDelay);
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
                        _tryDelay.Bucket.RemoveAwaiter(_tryDelay.Response.MessageGuid);
                    }}

                    _tryDelay.Bucket = null;
                    _tryDelay = null;
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