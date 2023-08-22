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
            TryProduce(sb, requestAwaiter);
            TryProduceDelay(sb, requestAwaiter);
            TryAddAwaiter(sb, requestAwaiter);
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
            private readonly Bucket[] {Buckets()};
            private uint {Current()};
");
        }

        private static string Buckets()
        {
            return "_buckets";
        }

        private static string Current()
        {
            return "_current";
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var bucketsParametr = "buckets";
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
                int {bucketsParametr},
                int {ProcessorConfig.MaxInFlyNameCamel()}
                {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} {ProcessorConfig.CurrentStateFuncNameCamel()}" : "")}
                {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.InputDatas)} {ProcessorConfig.AfterCommitFuncNameCamel()}" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                string {outputData.NameCamelCase}Name,
                {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");

                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@",{requestAwaiter.Data.AfterSendFunc(assemblyName, outputData, i)} {ProcessorConfig.AfterSendFuncNameCamel(outputData)}
");
                }

                if(requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.Data.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {ProcessorConfig.LoadOutputFuncNameCamel(outputData)},
                {requestAwaiter.Data.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {ProcessorConfig.CheckOutputStatusFuncNameCamel(outputData)}
");
                }
            }

            builder.Append($@"
                )
            {{
                {Buckets()} = new Bucket[{bucketsParametr}];
                for (int bucketId = 0; bucketId < {bucketsParametr}; bucketId++)
                {{
                    {Buckets()}[bucketId] = new Bucket(
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
                        {ProcessorConfig.MaxInFlyNameCamel()}
                        {(requestAwaiter.Data.UseLogger ? @",logger" : "")}
                        {(consumerData.CheckCurrentState ? $",{ProcessorConfig.CurrentStateFuncNameCamel()}" : "")}
                        {(consumerData.UseAfterCommit ? $",{ProcessorConfig.AfterCommitFuncNameCamel()}" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                        {outputData.NameCamelCase}Name,
                        producerPool{i}
");

                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@",{ProcessorConfig.AfterSendFuncNameCamel(outputData)}
");
                }

                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                        {ProcessorConfig.LoadOutputFuncNameCamel(outputData)},
                        {ProcessorConfig.CheckOutputStatusFuncNameCamel(outputData)}
");
                }
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
                for (int i = 0; i < {Buckets()}.Length; i++)
                {{
                    {Buckets()}[i].Start(bootstrapServers, groupId, changeConfig);
                }}
            }}
");
        }

        private static void DisposeAsync(StringBuilder builder)
        {
            builder.Append($@"
            public async ValueTask DisposeAsync()
            {{
                for (int i = 0; i < {Buckets()}.Length; i++)
                {{
                    await {Buckets()}[i].DisposeAsync();
                }}
            }}
");
        }

        private static void TryProduce(
            StringBuilder builder, 
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async ValueTask<{requestAwaiter.TypeSymbol.Name}.TryProduceResult> TryProduce(
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
                for (int i = 0; i < {Buckets()}.Length; i++)
                {{
                    var index = {Current()};
                    var tp = await {Buckets()}[index].TryProduce(
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

                    uint nextIndex = (index + 1) % (uint){Buckets()}.Length;
                    Interlocked.CompareExchange(ref {Current()}, nextIndex, index);
                }}

                return new {requestAwaiter.TypeSymbol.Name}.TryProduceResult {{ Succsess = false }};
            }}
");
        }

        private static void TryProduceDelay(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult TryProduceDelay(
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
                for (int i = 0; i < {Buckets()}.Length; i++)
                {{
                    var index = {Current()};
                    var tp = {Buckets()}[index].TryProduceDelay(
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
                    );
                    if (tp.Succsess)
                    {{
                        return tp;
                    }}

                    uint nextIndex = (index + 1) % (uint){Buckets()}.Length;
                    Interlocked.CompareExchange(ref {Current()}, nextIndex, index);
                }}

                return new {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult {{ Succsess = false }};
            }}
");
        }

        private static void TryAddAwaiter(
            StringBuilder builder,
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
                for (int i = 0; i < {Buckets()}.Length; i++)
                {{
                    var currentBucket = {Buckets()}[i];
                    if(currentBucket.BucketId != bucket)
                    {{
                        continue;
                    }}

                    var containAll = true;
");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                    for(int j = 0; j < input{i}Partitions.Length; j++)
                    {{
                        var containPartition = false;
                        for (int z = 0; z < currentBucket.{inputData.NamePascalCase}Partitions.Length; z++)
                        {{
                            containPartition = input{i}Partitions[j] == currentBucket.{inputData.NamePascalCase}Partitions[z];
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