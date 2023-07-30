using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class PartitionItem
    {
        public static void Append(
            StringBuilder sb,
            string assemblyName,
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            StartClassPartitionItem(sb, assemblyName, requestAwaiter);

            Bucket.Append(sb, assemblyName, requestAwaiter);

            Constructor(sb, assemblyName, requestAwaiter);
            Start(sb);
            StopPartitionItem(sb);
            TryProduce(sb, assemblyName, requestAwaiter);
            End(sb);
        }

        private static void StartClassPartitionItem(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
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
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            builder.Append($@"
            public PartitionItem(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                string incomeTopic{i}Name,
                int[] incomeTopic{i}Partitions,
                string[] incomeTopic{i}CanAnswerService
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@",
                string outcomeTopic{i}Name,
                {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.Data.ProducerData;
            builder.Append($@",
                int buckets,
                int maxInFly
                {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState" : "")}
                {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.IncomeDatas)} afterCommit" : "")}
                {(producerData.CustomOutcomeHeader ? $@",{producerData.CustomOutcomeHeaderFunc(assemblyName)} createOutcomeHeader" : "")}
                {(producerData.CustomHeaders ? $@",{producerData.CustomHeadersFunc()} setHeaders" : "")}
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
                        {(requestAwaiter.Data.UseLogger ? @",logger" : "")}
                        {(consumerData.CheckCurrentState ? $",getCurrentState" : "")}
                        {(consumerData.UseAfterCommit ? $",afterCommit" : "")}
                        {(producerData.CustomOutcomeHeader ? $@",createOutcomeHeader" : "")}
                        {(producerData.CustomHeaders ? $@",setHeaders" : "")}
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
                string groupId,
                Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                )
            {{
                for (int i = 0; i < _buckets.Length; i++)
                {{
                    _buckets[i].Start(bootstrapServers, groupId, changeConfig);
                }}
            }}
");
        }

        private static void StopPartitionItem(StringBuilder builder)
        {
            builder.Append($@"
            public void Stop()
            {{
                for (int i = 0; i < _buckets.Length; i++)
                {{
                    _buckets[i].StopConsume();
                    _buckets[i].Dispose();
                }}
            }}
");
        }

        private static void TryProduce(
            StringBuilder builder, 
            string assemblyName, 
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            builder.Append($@"
            public async ValueTask<{assemblyName}.TryProduceResult> TryProduce(
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
                for (int i = 0; i < _buckets.Length; i++)
                {{
                    var index = _current;
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
                    ).ConfigureAwait(false);
                    if (tp.Succsess)
                    {{
                        return tp;
                    }}

                    uint nextIndex = (index + 1) % (uint)_buckets.Length;
                    Interlocked.CompareExchange(ref _current, nextIndex, index);
                }}

                return new {assemblyName}.TryProduceResult {{ Succsess = false }};
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