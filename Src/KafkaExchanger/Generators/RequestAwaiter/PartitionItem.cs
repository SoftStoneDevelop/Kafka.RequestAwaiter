using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class PartitionItem
    {
        public static void Append(
            StringBuilder sb,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            StartClassPartitionItem(sb, assemblyName, requestAwaiter);

            Bucket.Append(sb, assemblyName, requestAwaiter);

            Constructor(sb, assemblyName, requestAwaiter);
            Start(sb);
            StopPartitionItem(sb);
            Produce(sb, assemblyName, requestAwaiter);
            End(sb);
        }

        private static void StartClassPartitionItem(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        private class PartitionItem
        {{
            private readonly Bucket[] _buckets;
            private uint _current;
");
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public PartitionItem(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                string incomeTopic{i}Name,
                int[] incomeTopic{i}Partitions,
                string[] incomeTopic{i}CanAnswerService,
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@"
                string outcomeTopic{i}Name,
                {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }

            builder.Append($@"
                {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? $@",Func<Task<{assemblyName}.ResponseHeader>> createOutcomeHeader" : "")}
                {(requestAwaiter.Data.ProducerData.CustomHeaders ? @",Func<Headers, Task> setHeaders" : "")}
                ,int buckets,
                int maxInFly
                )
            {{
                _buckets = new Bucket[buckets];
                for (int i = 0; i < buckets; i++)
                {{
                    _buckets[i] = new Bucket(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                        incomeTopic{i}Name,
                        incomeTopic{i}Partitions,
                        incomeTopic{i}CanAnswerService,
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@"
                        outcomeTopic{i}Name,
                        producerPool{i},
");
            }

            builder.Append($@"
                        i,
                        maxInFly
                        );
                }}
            }}
");
        }

        private static void Start(
            StringBuilder builder
            )
        {
            builder.Append($@"
            public void Start(
                string bootstrapServers,
                string groupId
                )
            {{
                for (int i = 0; i < _buckets.Length; i++)
                {{
                    _buckets[i].Start(bootstrapServers, groupId);
                }}
            }}
");
        }

        private static void StopPartitionItem(StringBuilder builder)
        {
            builder.Append($@"
            public async Task Stop()
            {{
                for (int i = 0; i < _buckets.Length; i++)
                {{
                    await _buckets[i].StopConsume();
                    _buckets[i].Dispose();
                }}
            }}
");
        }

        private static void Produce(
            StringBuilder builder, 
            string assemblyName, 
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async Task<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeData = requestAwaiter.OutcomeDatas[i];
                if (!outcomeData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                {outcomeData.KeyType.GetFullTypeName(true)} key{i},
");
                }

                builder.Append($@"
                {outcomeData.ValueType.GetFullTypeName(true)} value{i},
");
            }
            builder.Append($@"
                int waitResponseTimeout = 0
                )
            {{
                while (true)
                {{
                    for (int i = 0; i < _buckets.Length; i++)
                    {{
                        var index = Interlocked.Increment(ref _current) % _buckets.Length;
                        var tp = await _buckets[index].TryProduce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeData = requestAwaiter.OutcomeDatas[i];
                if (!outcomeData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                        key{i},
");
                }

                builder.Append($@"
                        value{i},
");
            }
            builder.Append($@"
                        waitResponseTimeout
                        );
                        if (tp.Succsess)
                        {{
                            return tp.Response;
                        }}
                    }}
                }}
            }}
");
        }

        private static void End(StringBuilder builder)
        {
            builder.Append($@"
        }}
");
        }
    }
}