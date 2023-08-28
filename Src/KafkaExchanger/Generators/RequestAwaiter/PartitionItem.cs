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
            StopAsync(sb);
            TryProduceDelay(sb, requestAwaiter);
            TryAddAwaiter(sb, requestAwaiter);
            End(sb);
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
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
        public class {TypeName()}
        {{
            private readonly {Bucket.TypeFullName(requestAwaiter)}[] {_buckets()};
            private uint {_current()};
");
        }

        private static string _buckets()
        {
            return "_buckets";
        }

        private static string _current()
        {
            return "_current";
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            string checkOutputStatusFunc(OutputData outputData)
            {
                return $"check{outputData.NamePascalCase}Status";
            }

            string loadOutputFunc(OutputData outputData)
            {
                return $"load{outputData.MessageTypeName}";
            }

            string afterSendFunc(OutputData outputData)
            {
                return $"afterSend{outputData.NamePascalCase}";
            }

            string currentStateFunc()
            {
                return $"currentState";
            }

            string afterCommitFunc()
            {
                return $"afterCommit";
            }

            var bucketsParametr = "buckets";
            var msxInFlyParametr = "maxInFly";
            var fwsParametr = "fws";
            builder.Append($@"
            public {TypeName()}(
                KafkaExchanger.FreeWatcherSignal {fwsParametr},
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                if (i != 0)
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
                {_buckets()} = new {Bucket.TypeFullName(requestAwaiter)}[{bucketsParametr}];
                for (int bucketId = 0; bucketId < {bucketsParametr}; bucketId++)
                {{
                    {_buckets()}[bucketId] = new {Bucket.TypeFullName(requestAwaiter)}(
                        {fwsParametr},
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
                for (int i = 0; i < {_buckets()}.Length; i++)
                {{
                    {_buckets()}[i].Start(bootstrapServers, groupId, changeConfig);
                }}
            }}
");
        }

        private static void StopAsync(StringBuilder builder)
        {
            builder.Append($@"
            public async ValueTask StopAsync(CancellationToken token = default)
            {{
                for (int i = 0; i < {_buckets()}.Length; i++)
                {{
                    await {_buckets()}[i].StopAsync(token);
                }}
            }}
");
        }

        private static void TryProduceDelay(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public {TryDelayProduceResult.TypeFullName(requestAwaiter)} TryProduceDelay(
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
                for (int i = 0; i < {_buckets()}.Length; i++)
                {{
                    var index = {_current()};
                    var tp = {_buckets()}[index].TryProduceDelay(
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

                    uint nextIndex = (index + 1) % (uint){_buckets()}.Length;
                    Interlocked.CompareExchange(ref {_current()}, nextIndex, index);
                }}

                return new {TryDelayProduceResult.TypeFullName(requestAwaiter)}
                {{ 
                    {TryDelayProduceResult.Succsess()}  = false 
                }};
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
                    $"async ValueTask<{TryAddAwaiterResult.TypeFullName(requestAwaiter)}>" : 
                    $"{TryAddAwaiterResult.TypeFullName(requestAwaiter)}"
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
                for (int i = 0; i < {_buckets()}.Length; i++)
                {{
                    var currentBucket = {_buckets()}[i];
                    if(currentBucket.{Bucket.BucketId()} != bucket)
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
                        for (int z = 0; z < currentBucket.{Bucket.Partitions(inputData)}.Length; z++)
                        {{
                            containPartition = input{i}Partitions[j] == currentBucket.{Bucket.Partitions(inputData)}[z];
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
                    
                    return new {TryAddAwaiterResult.TypeFullName(requestAwaiter)}() 
                    {{
                        {TryAddAwaiterResult.Succsess()} = true,
                        {TryAddAwaiterResult.Response()} = result
                    }};
                }}

                return new {TryAddAwaiterResult.TypeFullName(requestAwaiter)}()
                {{
                    {TryAddAwaiterResult.Succsess()} = false
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