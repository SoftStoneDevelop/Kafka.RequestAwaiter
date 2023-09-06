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
            StartClass(sb, assemblyName, requestAwaiter);

            PropertiesAndFields(sb, assemblyName, requestAwaiter);
            Constructor(sb, assemblyName, requestAwaiter);

            Start(sb, requestAwaiter);
            Setup(sb, requestAwaiter);
            StartCommitRoutine(sb, requestAwaiter);
            StartInitializeRoutine(sb, requestAwaiter);
            StartConsumeInput(sb, assemblyName, requestAwaiter);

            StopAsync(sb);
            ProduceDelay(sb, requestAwaiter);
            Produce(sb, requestAwaiter);
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

        private static void StartClass(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{");
        }

        private static string _current()
        {
            return "_current";
        }

        private static string _needCommit()
        {
            return "_needCommit";
        }

        private static string _commitOffsets()
        {
            return "_commitOffsets";
        }

        private static string _tcsCommit()
        {
            return "_tcsCommit";
        }

        private static string _cts()
        {
            return "_cts";
        }

        private static string _afterCommit()
        {
            return "_afterCommit";
        }

        private static string _currentState()
        {
            return "_currentState";
        }

        private static string _logger()
        {
            return "_logger";
        }

        private static string _loadOutputMessage(OutputData outputData)
        {
            return $@"_load{outputData.NamePascalCase}Message";
        }

        private static string _checkOutputStatus(OutputData outputData)
        {
            return $@"_check{outputData.NamePascalCase}Status";
        }

        private static string _inputTopicName(InputData inputData)
        {
            return $@"_{inputData.NameCamelCase}Name";
        }

        private static string _inputTopicPartitions(InputData inputData)
        {
            return $@"_{inputData.NameCamelCase}Partitions";
        }

        private static string _itemsInBucket()
        {
            return "_itemsInBucket";
        }

        private static string _inFlyItemsLimit()
        {
            return "_inFlyItemsLimit";
        }

        private static string _storage()
        {
            return "_storage";
        }

        private static string _afterSendOutput(OutputData outputData)
        {
            return $"_afterSend{outputData.NamePascalCase}";
        }

        private static string _outputTopicName(OutputData outputData)
        {
            return $@"_{outputData.NameCamelCase}Name";
        }

        private static string _outputPool(OutputData outputData)
        {
            return $@"_{outputData.NameCamelCase}Pool";
        }

        private static string _consumeRoutines()
        {
            return "_consumeRoutines";
        }

        private static string _horizonRoutine()
        {
            return "_horizonRoutine";
        }

        private static string _initializeRoutine()
        {
            return "_initializeRoutine";
        }

        private static string _responseAwaiters()
        {
            return "_responseAwaiters";
        }

        private static string _channel()
        {
            return "_channel";
        }

        private static string _initializeChannel()
        {
            return "_initializeChannel";
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

            string inputTopicName(InputData inputData)
            {
                return $@"{inputData.NameCamelCase}Name";
            }

            string inputTopicPartitions(InputData inputData)
            {
                return $@"{inputData.NameCamelCase}Partitions";
            }

            string outputTopicName(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Name";
            }

            string outputPool(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Pool";
            }

            var inFlyBucketsLimitParam = "inFlyBucketsLimit";
            var itemsInBucketParam = "itemsInBucket";
            var addNewBucketParam = "addNewBucket";
            builder.Append($@"
            public {TypeName()}(
                int {inFlyBucketsLimitParam},
                int {itemsInBucketParam},
                {requestAwaiter.AddNewBucketFuncType()} {addNewBucketParam},");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                string {inputTopicName(inputData)},
                int[] {inputTopicPartitions(inputData)}");
            }

            var loggerParam = "logger";
            if(requestAwaiter.UseLogger)
            {
                builder.Append($@",
                ILogger {loggerParam}");
            }

            if(requestAwaiter.CheckCurrentState)
            {
                builder.Append($@",
                {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {currentStateFunc()}");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@",
                {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {afterCommitFunc()}");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                string {outputTopicName(outputData)},
                {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} {outputPool(outputData)}");

                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",
                {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {afterSendFunc(outputData)}");
                }

                if(requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {loadOutputFunc(outputData)},
                {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {checkOutputStatusFunc(outputData)}");

                }
            }

            builder.Append($@"
                )
            {{
                {_itemsInBucket()} = {itemsInBucketParam};
                {_inFlyItemsLimit()} = {inFlyBucketsLimitParam} * {itemsInBucketParam};
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                        {_inputTopicName(inputData)} = {inputTopicName(inputData)};
                        {_inputTopicPartitions(inputData)} = {inputTopicPartitions(inputData)};");
            }

            if(requestAwaiter.UseLogger)
            {
                builder.Append($@"
                {_logger()} = {loggerParam};");
            }

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                {_currentState()} = {currentStateFunc()};");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                {_afterCommit()} = {afterCommitFunc()};");
            }
            
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                {_outputTopicName(outputData)} = {outputTopicName(outputData)};
                {_outputPool(outputData)} = {outputPool(outputData)};");

                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                {_afterSendOutput(outputData)} = {afterSendFunc(outputData)};");

                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                {_loadOutputMessage(outputData)} = {loadOutputFunc(outputData)};
                {_checkOutputStatus(outputData)} = {checkOutputStatusFunc(outputData)};");

                }
            }

            builder.Append($@"
                {_storage()} = new KafkaExchanger.BucketStorage(
                    inFlyLimit: {inFlyBucketsLimitParam},
                    inputs: {requestAwaiter.InputDatas.Count},
                    itemsInBucket: {_itemsInBucket()},
                    addNewBucket: async (bucketId) =>
                    {{
                        await {addNewBucketParam}(
                            bucketId,");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                            {_inputTopicPartitions(inputData)},
                            {_inputTopicName(inputData)}");
            }
            builder.Append($@"
                            );
                    }}
                    );
            }}
");
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private uint {_current()};
            private int {_needCommit()};
            private Confluent.Kafka.TopicPartitionOffset[] {_commitOffsets()};
            private System.Threading.Tasks.TaskCompletionSource {_tcsCommit()} = new();
            private System.Threading.CancellationTokenSource {_cts()};
            private System.Threading.Thread[] {_consumeRoutines()};
            private System.Threading.Tasks.Task {_horizonRoutine()};
            private System.Threading.Tasks.Task {_initializeRoutine()};

            private readonly ConcurrentDictionary<string, {TopicResponse.TypeFullName(requestAwaiter)}> {_responseAwaiters()};
            private readonly Channel<{ChannelInfo.TypeFullName(requestAwaiter)}> {_channel()} = Channel.CreateUnbounded<{ChannelInfo.TypeFullName(requestAwaiter)}>(
                new UnboundedChannelOptions()
                {{
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = false
                }});

            private readonly Channel<{TopicResponse.TypeFullName(requestAwaiter)}> {_initializeChannel()} = Channel.CreateUnbounded<{TopicResponse.TypeFullName(requestAwaiter)}>(
                new UnboundedChannelOptions()
                {{
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = true
                }});

            private readonly KafkaExchanger.BucketStorage {_storage()};
            private readonly int {_itemsInBucket()};
            private readonly int {_inFlyItemsLimit()};
");
            if(requestAwaiter.UseLogger)
            {
                builder.Append($@"
            private readonly ILogger {_logger()};");
            }

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
            private readonly {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {_currentState()};");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
            private readonly {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {_afterCommit()};");
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            private readonly string {_inputTopicName(inputData)};
            private readonly int[] {_inputTopicPartitions(inputData)};");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            private readonly string {_outputTopicName(outputData)};
            private readonly {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} {_outputPool(outputData)};");

                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
            private readonly {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {_afterSendOutput(outputData)};");

                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
            private readonly {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {_loadOutputMessage(outputData)};
            private readonly {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {_checkOutputStatus(outputData)};");

                }
            }
        }

        private static void Start(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void Start(
                string bootstrapServers,
                string groupId,
                Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                )
            {{
                {_cts()} = new CancellationTokenSource();
                {_storage()}.Validate();
                StartCommitRoutine();
                StartInitializeRoutine();
                {_consumeRoutines()} = new Thread[{requestAwaiter.InputDatas.Count}];");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                _consumeRoutines[{i}] = StartConsume{inputData.NamePascalCase}(bootstrapServers, groupId, changeConfig);
                _consumeRoutines[{i}].Start();
");
            }
            builder.Append($@"
            }}
");
        }

        private static void Setup(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async Task Setup(
                {requestAwaiter.BucketsCountFuncType()} currentBucketsCount
                )
            {{
                await _storage.Init(currentBucketsCount: async () =>
                {{
                    return await currentBucketsCount(");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                            {_inputTopicPartitions(inputData)},
                            {_inputTopicName(inputData)}");
            }
            builder.Append($@"
                            );
                }}
                );
            }}
");
        }

        public static void StartCommitRoutine(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private void StartCommitRoutine()
            {{
                {_horizonRoutine()} = Task.Factory.StartNew(async () => 
                {{
                    var reader = {_channel()}.Reader;
                    var writer = {_initializeChannel()}.Writer;

                    var queue = new Queue<{StartResponse.TypeFullName(requestAwaiter)}>();
                    var inTheFlyCount = 0;
                    try
                    {{
                        var consumeStop = false;
                        while (!{_cts()}.Token.IsCancellationRequested)
                        {{
                            var info = await reader.ReadAsync({_cts()}.Token).ConfigureAwait(false);
                            if (info is {StartResponse.TypeFullName(requestAwaiter)} startResponse)
                            {{
                                if (queue.Count != 0 || inTheFlyCount == {_inFlyItemsLimit()})
                                {{
                                    queue.Enqueue(startResponse);
                                }}

                                var newMessage = new KafkaExchanger.MessageInfo({requestAwaiter.InputDatas.Count});
                                var bucketId = await {_storage()}.Push(startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Guid()}, newMessage);
                                startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Bucket()} = bucketId;
                                await writer.WriteAsync(startResponse.{StartResponse.ResponseProcess()});
                                inTheFlyCount++;
                            }}
                            else if (info is {SetOffsetResponse.TypeFullName(requestAwaiter)} setOffsetResponse)
                            {{
                                _ = {_storage()}.SetOffset(
                                    setOffsetResponse.{SetOffsetResponse.BucketId()},
                                    setOffsetResponse.{SetOffsetResponse.Guid()},
                                    setOffsetResponse.{SetOffsetResponse.OffsetId()},
                                    setOffsetResponse.{SetOffsetResponse.Offset()}
                                    );
                            }}
                            else if (info is {EndResponse.TypeFullName(requestAwaiter)} endResponse)
                            {{
                                {_storage()}.Finish(endResponse.{EndResponse.BucketId()}, endResponse.{EndResponse.Guid()});

                                var canFreeBuckets = {_storage()}.CanFreeBuckets();
                                if(canFreeBuckets.Count == 0)
                                {{
                                    continue;
                                }}

                                inTheFlyCount -= canFreeBuckets.Count * {_itemsInBucket()};
                                while (queue.Count != 0)
                                {{
                                    if (inTheFlyCount == {_inFlyItemsLimit()})
                                    {{
                                        break;
                                    }}

                                    startResponse = queue.Dequeue();
                                    var newMessage = new KafkaExchanger.MessageInfo({requestAwaiter.InputDatas.Count});
                                    var bucketId = await _storage.Push(startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Guid()}, newMessage);
                                    startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Bucket()} = bucketId;
                                    await writer.WriteAsync(startResponse.{StartResponse.ResponseProcess()});
                                    inTheFlyCount++;
                                }}

                                var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                                var offset = canFreeBuckets[^1].MaxOffset;
                                Volatile.Write(ref {_commitOffsets()}, offset);
                                Interlocked.Exchange(ref {_tcsCommit()}, commit);
                                Interlocked.Exchange(ref {_needCommit()}, 1);

                                await commit.Task.ConfigureAwait(false);");
            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                                for (int i = 0; i < canFreeBuckets.Count; i++)
                                {{
                                    var freeBucket = canFreeBuckets[i];
                                    await {_afterCommit()}(
                                        freeBucket.BucketId");
                for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
                {
                    var inputData = requestAwaiter.InputDatas[i];
                    builder.Append($@",
                                        {_inputTopicPartitions(inputData)}");
                }
                builder.Append($@"
                                        ).ConfigureAwait(false);
                                }}");
            }
            builder.Append($@"
                            }}
                            else
                            {{
                                {(requestAwaiter.UseLogger ? $@"{_logger()}.LogError(""Unknown info type"");" : "//ignore")}
                            }}
                        }}
                    }}
                    catch (OperationCanceledException)
                    {{
                        //ignore
                    }}
                    catch (ChannelClosedException)
                    {{
                        //ignore
                    }}
                    catch (Exception {(requestAwaiter.UseLogger ? $"ex" : string.Empty)})
                    {{
                        {(requestAwaiter.UseLogger ? $@"{_logger()}.LogError(ex, ""Error commit task"");" : string.Empty)}
                        throw;
                    }}
                }});
            }}
");
        }

        public static void StartInitializeRoutine(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private void StartInitializeRoutine()
            {{
                {_initializeRoutine()} = Task.Factory.StartNew(async () => 
                {{
                    var reader = {_initializeChannel()}.Reader;
                    try
                    {{
                        while (!{_cts()}.Token.IsCancellationRequested)
                        {{
                            var propessResponse = await reader.ReadAsync({_cts()}.Token).ConfigureAwait(false);
                            propessResponse.Init();
                        }}
                    }}
                    catch (Exception {(requestAwaiter.UseLogger ? $"ex" : string.Empty)})
                    {{
                        {(requestAwaiter.UseLogger ? $@"{_logger()}.LogError(ex, ""Error init task"");" : string.Empty)}
                        throw;
                    }}
                }});
            }}
");
        }

        public static void StartConsumeInput(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                var threadName = $@"{requestAwaiter.TypeSymbol.Name}{{groupId}}{_inputTopicName(inputData)}";
                builder.Append($@"
            private Thread StartConsume{inputData.NamePascalCase}(
                string bootstrapServers,
                string groupId,
                Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                )
            {{
                return new Thread((param) =>
                {{
                    start:
                    if({_cts()}.Token.IsCancellationRequested)
                    {{
                        return;
                    }}

                    try
                    {{
                        var conf = new Confluent.Kafka.ConsumerConfig();
                        if(changeConfig != null)
                        {{
                            changeConfig(conf);
                        }}

                        conf.GroupId = groupId;
                        conf.BootstrapServers = bootstrapServers;
                        conf.AutoOffsetReset = AutoOffsetReset.Earliest;
                        conf.AllowAutoCreateTopics = false;
                        conf.EnableAutoCommit = false;

                        var consumer =
                            new ConsumerBuilder<{inputData.TypesPair}>(conf)
                            .Build()
                            ;

                        consumer.Assign({_inputTopicPartitions(inputData)}.Select(sel => new Confluent.Kafka.TopicPartition({_inputTopicName(inputData)}, sel)));

                        try
                        {{
                            while (!_cts.Token.IsCancellationRequested)
                            {{
                                try
                                {{
                                    var consumeResult = consumer.Consume(50);
                                    if (Interlocked.CompareExchange(ref {_needCommit()}, 0, 1) == 1)
                                    {{
                                        var offsets = Volatile.Read(ref {_commitOffsets()});
                                        consumer.Commit(offsets);
                                        Volatile.Read(ref {_tcsCommit()}).SetResult();
                                    }}

                                    if (consumeResult == null)
                                    {{
                                        continue;
                                    }}

                                    var inputMessage = new {inputData.MessageTypeName}();
                                    inputMessage.{BaseInputMessage.TopicPartitionOffset()} = consumeResult.TopicPartitionOffset;
                                    inputMessage.{InputMessages.OriginalMessage()} = consumeResult.Message;
");
                if (inputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@"
                                    inputMessage.{InputMessages.Key()} = {inputData.KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan());
");
                }

                if (inputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@"
                                    inputMessage.{InputMessages.Value()} = {inputData.ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan());
");
                }

                builder.Append($@"
                                    if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                    {{
                                        continue;
                                    }}

                                    inputMessage.{InputMessages.Header()} = {assemblyName}.ResponseHeader.Parser.ParseFrom(infoBytes);
                                    if (!{_responseAwaiters()}.TryGetValue(inputMessage.{InputMessages.Header()}.AnswerToMessageGuid, out var awaiter))
                                    {{
                                        continue;
                                    }}");
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                                    awaiter.TrySetResponse({inputData.Id}, inputMessage);");
                }
                else
                {
                    builder.Append($@"
                                    switch(inputMessage.Header.AnswerFrom)
                                    {{
                                        default:
                                        {{
                                            //ignore
                                            break;
                                        }}
");

                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        var acceptedService = inputData.AcceptedService[j];
                        var acceptedServiceId = j;
                        builder.Append($@"
                                        case ""{acceptedService}"":
                                        {{
                                            awaiter.TrySetResponse({inputData.Id}, inputMessage, {acceptedServiceId});
                                            break;
                                        }}");
                    }
                    builder.Append($@"
                                    }}");
                }
                builder.Append($@"
                                }}
                                catch (ConsumeException)
                                {{
                                    throw;
                                }}
                            }}
                        }}
                        catch (OperationCanceledException)
                        {{
                            consumer.Close();
                        }}
                        finally
                        {{
                            consumer.Dispose();
                        }}
                    }}
                    catch (Exception {(requestAwaiter.UseLogger ? $"ex" : string.Empty)})
                    {{
                        {(requestAwaiter.UseLogger ? $@"{_logger()}.LogError(ex, $""{threadName}"");" : string.Empty)}
                        goto start;
                    }}
                }}
                    )
                    {{
                        IsBackground = true,
                        Priority = ThreadPriority.AboveNormal,
                        Name = $""{threadName}""
                    }};
            }}
");
            }
        }

        private static void StopAsync(StringBuilder builder)
        {
            builder.Append($@"
            public async ValueTask StopAsync(CancellationToken token = default)
            {{
                
            }}
");
        }

        private static void ProduceDelay(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public Task<{DelayProduce.TypeFullName(requestAwaiter)}> ProduceDelay()
            {{
");
            builder.Append($@"
            }}
");
        }

        private static void Produce(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public Task<{Response.TypeFullName(requestAwaiter)}> Produce()
            {{
");
            builder.Append($@"
            }}
");
        }

        private static void TryAddAwaiter(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public Task<{TryAddAwaiterResult.TypeFullName(requestAwaiter)}> TryAddAwaiter(
                string messageGuid,
                int bucket
                )
            {{
");
            builder.Append($@"
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