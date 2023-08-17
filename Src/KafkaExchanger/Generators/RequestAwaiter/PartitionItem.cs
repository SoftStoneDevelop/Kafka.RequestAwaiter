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
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            StartClassPartitionItem(sb, assemblyName, requestAwaiter);

            Bucket.Append(sb, assemblyName, requestAwaiter);

            Constructor(sb, assemblyName, requestAwaiter);
            Start(sb);
            DisposeAsync(sb);
            TryProduce(sb, assemblyName, requestAwaiter);
            TryProduceDelay(sb, assemblyName, requestAwaiter);
            TryAddAwaiter(sb, assemblyName, requestAwaiter);
            End(sb);
        }

        private static void StartClassPartitionItem(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class PartitionItem : IAsyncDisposable
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
            var consumerData = requestAwaiter.Data.ConsumerData;
            builder.Append($@",
                int buckets,
                int maxInFly
                {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState" : "")}
                {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.InputDatas)} afterCommit" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                string outputTopic{i}Name,
                {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");

                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@",{requestAwaiter.Data.AfterSendFunc(assemblyName, outputData, i)} afterSendOutput{i}
");
                }

                if(requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@",{requestAwaiter.Data.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} loadOutput{i}Message
");
                }
            }

            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@",{requestAwaiter.Data.AddAwaiterCheckStatusFunc(assemblyName, requestAwaiter.InputDatas)} addAwaiterCheckStatus
");
            }

            builder.Append($@"
                )
            {{
                _buckets = new Bucket[buckets];
                for (int bucketId = 0; bucketId < buckets; bucketId++)
                {{
                    _buckets[bucketId] = new Bucket(
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                        inputTopic{i}Name,
                        inputTopic{i}Partitions,
");
            }

            builder.Append($@"
                        bucketId,
                        maxInFly
                        {(requestAwaiter.Data.UseLogger ? @",logger" : "")}
                        {(consumerData.CheckCurrentState ? $",getCurrentState" : "")}
                        {(consumerData.UseAfterCommit ? $",afterCommit" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@",
                        outputTopic{i}Name,
                        producerPool{i}
");

                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@",afterSendOutput{i}
");
                }
                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@",loadOutput{i}Message
");
                }
            }

            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@",addAwaiterCheckStatus
");
            }
            builder.Append($@"
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

        private static void DisposeAsync(StringBuilder builder)
        {
            builder.Append($@"
            public async ValueTask DisposeAsync()
            {{
                for (int i = 0; i < _buckets.Length; i++)
                {{
                    await _buckets[i].DisposeAsync();
                }}
            }}
");
        }

        private static void TryProduce(
            StringBuilder builder, 
            string assemblyName, 
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
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

        private static void TryProduceDelay(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async ValueTask<{requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult> TryProduceDelay(
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
                    var tp = await _buckets[index].TryProduceDelay(
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

                return new {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult {{ Succsess = false }};
            }}
");
        }

        private static void TryAddAwaiter(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var returnType = 
                requestAwaiter.Data.AddAwaiterCheckStatus ? 
                    $"async ValueTask<{requestAwaiter.Data.TypeSymbol.Name}.TryAddAwaiterResult>" : 
                    $"{requestAwaiter.Data.TypeSymbol.Name}.TryAddAwaiterResult"
                    ;
            builder.Append($@"
            public {returnType} TryAddAwaiter(
                string messageGuid,
                int bucket,
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                int[] input{i}Partitions,
");
            }
            builder.Append($@"
                int waitResponseTimeout = 0
                )
            {{
                for (int i = 0; i < _buckets.Length; i++)
                {{
                    var currentBucket = _buckets[i];
                    if(currentBucket.BucketId != bucket)
                    {{
                        continue;
                    }}

                    var containAll = true;
");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                    for(int j = 0; j < input{i}Partitions.Length; j++)
                    {{
                        var containPartition = false;
                        for (int z = 0; z < currentBucket.InputTopic{i}Partitions.Length; z++)
                        {{
                            containPartition = input{i}Partitions[j] == currentBucket.InputTopic{i}Partitions[z];
                            if (containPartition)
                            {{
                                break;
                            }}
                        }}

                        if(!containPartition)
                        {{
                            containAll = false;
                            break;
                        }}
                    }}

                    if(!containAll)
                    {{
                        //check next bucket
                        continue;
                    }}
");
            }
            builder.Append($@"
                    var result = {(requestAwaiter.Data.AddAwaiterCheckStatus ? "await " : "")}currentBucket.AddAwaiter(
                        messageGuid,
                        waitResponseTimeout
                        );
                    
                    return new TryAddAwaiterResult() 
                    {{
                        Succsess = true,
                        Response = result
                    }};
                }}

                return new {requestAwaiter.Data.TypeSymbol.Name}.TryAddAwaiterResult 
                {{
                    Succsess = false
                }};
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