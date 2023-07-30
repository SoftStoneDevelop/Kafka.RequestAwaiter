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
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                string inputTopic{i}Name,
                int[] inputTopic{i}Partitions
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@",
                string outputTopic{i}Name,
                {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.Data.ProducerData;
            builder.Append($@",
                int buckets,
                int maxInFly
                {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState" : "")}
                {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.InputDatas)} afterCommit" : "")}
                {(producerData.CustomOutputHeader ? $@",{producerData.CustomOutputHeaderFunc(assemblyName)} createOutputHeader" : "")}
                {(producerData.CustomHeaders ? $@",{producerData.CustomHeadersFunc()} setHeaders" : "")}
                )
            {{
                _buckets = new Bucket[buckets];
                for (int i = 0; i < buckets; i++)
                {{
                    _buckets[i] = new Bucket(
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                        inputTopic{i}Name,
                        inputTopic{i}Partitions,
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@"
                        outputTopic{i}Name,
                        producerPool{i},
");
            }

            builder.Append($@"
                        i,
                        maxInFly
                        {(requestAwaiter.Data.UseLogger ? @",logger" : "")}
                        {(consumerData.CheckCurrentState ? $",getCurrentState" : "")}
                        {(consumerData.UseAfterCommit ? $",afterCommit" : "")}
                        {(producerData.CustomOutputHeader ? $@",createOutputHeader" : "")}
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
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                {outputData.KeyType.GetFullTypeName(true)} key{i},
");
                }

                builder.Append($@"
                {outputData.ValueType.GetFullTypeName(true)} value{i},
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
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
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