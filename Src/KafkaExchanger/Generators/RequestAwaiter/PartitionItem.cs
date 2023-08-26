using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class PartitionItem
    {
        public static void Append(
            StringBuilder sb,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
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

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "PartitionItem";
        }

        private static void StartClassPartitionItem(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()} : IAsyncDisposable
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

        private static string loadOutputFunc(OutputData outputData)
        {
            return $"load{outputData.MessageTypeName}";
        }

        private static string afterSendFunc(OutputData outputData)
        {
            return $"afterSend{outputData.NamePascalCase}";
        }

        private static string currentStateFunc()
        {
            return $"currentState";
        }

        private static string afterCommitFunc()
        {
            return $"afterCommit";
        }

        private static string checkOutputStatusFunc(OutputData outputData)
        {
            return $"check{outputData.NamePascalCase}Status";
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            var bucketsParametr = "buckets";
            var msxInFlyParametr = "maxInFly";
            builder.Append($@"
            public {TypeName()}(
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
            builder.Append($@",
                int {bucketsParametr},
                int {msxInFlyParametr}
                {(requestAwaiter.UseLogger ? @",ILogger logger" : "")}
                {(requestAwaiter.CheckCurrentState ? $",{requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {currentStateFunc()}" : "")}
                {(requestAwaiter.AfterCommit ? $",{requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {afterCommitFunc()}" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                string {outputData.NameCamelCase}Name,
                {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");

                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",{requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {afterSendFunc(outputData)}
");
                }

                if(requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {loadOutputFunc(outputData)},
                {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {checkOutputStatusFunc(outputData)}
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
                        {msxInFlyParametr}
                        {(requestAwaiter.UseLogger ? @",logger" : "")}
                        {(requestAwaiter.CheckCurrentState ? $",{currentStateFunc()}" : "")}
                        {(requestAwaiter.AfterCommit ? $",{afterCommitFunc()}" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                        {outputData.NameCamelCase}Name,
                        producerPool{i}
");

                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",{afterSendFunc(outputData)}
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                        {loadOutputFunc(outputData)},
                        {checkOutputStatusFunc(outputData)}
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult TryProduceDelay(
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

                return new {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult {{ Succsess = false }};
            }}
");
        }

        private static void TryAddAwaiter(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            var returnType = 
                requestAwaiter.AddAwaiterCheckStatus ? 
                    $"async ValueTask<{requestAwaiter.TypeSymbol.Name}.TryAddAwaiterResult>" : 
                    $"{requestAwaiter.TypeSymbol.Name}.TryAddAwaiterResult"
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
                    var result = {(requestAwaiter.AddAwaiterCheckStatus ? "await " : "")}currentBucket.AddAwaiter(
                        messageGuid,
                        waitResponseTimeout
                        );
                    
                    return new TryAddAwaiterResult() 
                    {{
                        Succsess = true,
                        Response = result
                    }};
                }}

                return new {requestAwaiter.TypeSymbol.Name}.TryAddAwaiterResult 
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