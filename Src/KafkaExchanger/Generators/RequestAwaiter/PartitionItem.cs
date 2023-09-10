using KafkaExchanger.Datas;
using KafkaExchanger.Generators.Responder;
using KafkaExchanger.Helpers;
using System.Reflection;
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
            AddAwaiter(sb, requestAwaiter);
            Send(sb, assemblyName, requestAwaiter);

            RemoveAwaiter(sb, requestAwaiter);
            CreateOutput(sb, assemblyName, requestAwaiter);
            CreateOutputHeader(sb, assemblyName, requestAwaiter);

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

        public static string InputTopicPartitions(InputData inputData)
        {
            return $@"{inputData.NamePascalCase}Partitions";
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

        private static string _commitRoutine()
        {
            return "_commitRoutine";
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
                {requestAwaiter.AfterSendFunc(assemblyName, outputData)} {afterSendFunc(outputData)}");
                }

                if(requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData)} {loadOutputFunc(outputData)},
                {requestAwaiter.AddAwaiterStatusFunc(assemblyName)} {checkOutputStatusFunc(outputData)}");

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
                        {InputTopicPartitions(inputData)} = {inputTopicPartitions(inputData)};");
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
                    inputs: {InputsSize(requestAwaiter)},
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
                            {InputTopicPartitions(inputData)},
                            {_inputTopicName(inputData)}");
            }
            builder.Append($@"
                            );
                    }}
                    );
            }}
");
        }

        private static int InputsSize(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            var size = 0;
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(inputData.AcceptFromAny)
                {
                    size++;
                }
                else
                {
                    size += inputData.AcceptedService.Length;
                }
            }

            return size;
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private int {_needCommit()};
            private Confluent.Kafka.TopicPartitionOffset[] {_commitOffsets()};
            private System.Threading.Tasks.TaskCompletionSource {_tcsCommit()} = new();
            private System.Threading.CancellationTokenSource {_cts()};
            private System.Threading.Thread[] {_consumeRoutines()};
            private System.Threading.Tasks.Task {_commitRoutine()};
            private System.Threading.Tasks.Task {_initializeRoutine()};

            private readonly ConcurrentDictionary<string, {TopicResponse.TypeFullName(requestAwaiter)}> {_responseAwaiters()} = new();
            private readonly Channel<{ChannelInfo.TypeFullName(requestAwaiter)}> {_channel()} = Channel.CreateUnbounded<{ChannelInfo.TypeFullName(requestAwaiter)}>(
                new UnboundedChannelOptions()
                {{
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = false
                }});

            private readonly Channel<{StartResponse.TypeFullName(requestAwaiter)}> {_initializeChannel()} = Channel.CreateUnbounded<{StartResponse.TypeFullName(requestAwaiter)}>(
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
            public readonly int[] {InputTopicPartitions(inputData)};");
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
            private readonly {requestAwaiter.AfterSendFunc(assemblyName, outputData)} {_afterSendOutput(outputData)};");

                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
            private readonly {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData)} {_loadOutputMessage(outputData)};
            private readonly {requestAwaiter.AddAwaiterStatusFunc(assemblyName)} {_checkOutputStatus(outputData)};");

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
                            {InputTopicPartitions(inputData)},
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
                {_commitRoutine()} = Task.Factory.StartNew(async () => 
                {{
                    var reader = {_channel()}.Reader;
                    var writer = {_initializeChannel()}.Writer;

                    var queue = new Queue<{StartResponse.TypeFullName(requestAwaiter)}>();
                    var inTheFlyCount = 0;
                    try
                    {{
                        while (!{_cts()}.Token.IsCancellationRequested)
                        {{
                            var info = await reader.ReadAsync({_cts()}.Token).ConfigureAwait(false);
                            if (info is {StartResponse.TypeFullName(requestAwaiter)} startResponse)
                            {{
                                if (queue.Count != 0 || inTheFlyCount == {_inFlyItemsLimit()})
                                {{
                                    queue.Enqueue(startResponse);
                                }}

                                var newMessage = new KafkaExchanger.MessageInfo({InputsSize(requestAwaiter)});
                                var bucketId = await {_storage()}.Push(startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Guid()}, newMessage);
                                startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Bucket()} = bucketId;
                                await writer.WriteAsync(startResponse);
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
                                var commitOffsets = canFreeBuckets[^1].MaxOffset.Where(wh => wh != null).ToArray();
                                foreach (var popItem in canFreeBuckets)
                                {{
                                    {_storage()}.Pop(popItem);
                                }}

                                while (queue.Count != 0)
                                {{
                                    if (inTheFlyCount == {_inFlyItemsLimit()})
                                    {{
                                        break;
                                    }}

                                    startResponse = queue.Dequeue();
                                    var newMessage = new KafkaExchanger.MessageInfo({InputsSize(requestAwaiter)});
                                    var bucketId = await _storage.Push(startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Guid()}, newMessage);
                                    startResponse.{StartResponse.ResponseProcess()}.{TopicResponse.Bucket()} = bucketId;
                                    await writer.WriteAsync(startResponse);
                                    inTheFlyCount++;
                                }}

                                if(commitOffsets.Length != 0)
                                {{
                                    var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                                    Volatile.Write(ref {_commitOffsets()}, commitOffsets);
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
                                            {InputTopicPartitions(inputData)}");
                }
                builder.Append($@"
                                            ).ConfigureAwait(false);
                                    }}");
            }
            builder.Append($@"
                                }}
                                else
                                {{
                                    int s = 45;
                                }}");
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
                            var startResponse = await reader.ReadAsync({_cts()}.Token).ConfigureAwait(false);
                            startResponse.{StartResponse.ResponseProcess()}.Init();
                            startResponse.{StartResponse.Inited()}.SetResult();
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

                        consumer.Assign({InputTopicPartitions(inputData)}.Select(sel => new Confluent.Kafka.TopicPartition({_inputTopicName(inputData)}, sel)));

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

                                    var inputMessage = new {InputMessages.TypeFullName(requestAwaiter, inputData)}();
                                    inputMessage.{BaseInputMessage.TopicName()} = {_inputTopicName(inputData)};
                                    inputMessage.{BaseInputMessage.TopicPartitionOffset()} = consumeResult.TopicPartitionOffset;
                                    inputMessage.{InputMessages.OriginalMessage()} = consumeResult.Message;");
                if (inputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@"
                                    inputMessage.{InputMessages.Key()} = {inputData.KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan());");
                }

                if (inputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@"
                                    inputMessage.{InputMessages.Value()} = {inputData.ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan());");
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
            public async Task StopAsync(CancellationToken token = default)
            {{
                while(token == default || !token.IsCancellationRequested)
                {{
                    if({_responseAwaiters()}.Count == 0)
                    {{
                        break;
                    }}

                    await Task.Delay(25);
                }}

                {_cts()}?.Cancel();

                foreach (var consumeRoutine in {_consumeRoutines()})
                {{
                    while (consumeRoutine.IsAlive)
                    {{
                        await Task.Delay(15);
                    }}
                }}

                {_tcsCommit()}.TrySetCanceled();
                {_channel()}.Writer.Complete();
                {_initializeChannel()}.Writer.Complete();

                try
                {{
                    await {_commitRoutine()};
                }}
                catch
                {{
                    //ignore
                }}

                try
                {{
                    await {_initializeRoutine()};
                }}
                catch
                {{
                    //ignore
                }}

                foreach(var awaiter in {_responseAwaiters()}.Values)
                {{
                    try
                    {{
                        awaiter.Dispose(); 
                    }}
                    catch
                    {{ 
                        //ignore
                    }}
                }}

                {_cts()}?.Dispose();
            }}
");
        }

        private static void ProduceDelay(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            string outputKeyParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Key";
            }

            string outputValueParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Value";
            }

            builder.Append($@"
            public async Task<{DelayProduce.TypeFullName(requestAwaiter)}> ProduceDelay(");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                {outputData.KeyType.GetFullTypeName(true)} {outputKeyParam(outputData)},");
                }

                builder.Append($@"
                {outputData.ValueType.GetFullTypeName(true)} {outputValueParam(outputData)}");
            }

            builder.Append($@",
                int waitResponseTimeout = 0
                )
            {{
                var guid = Guid.NewGuid().ToString(""D"");");
            CreateTopicResponse(
                builder,
                "                ",
                @"guid",
                requestAwaiter
                );
            builder.Append($@"
                var output = CreateOutput(
                    guid");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@",
                    {outputKeyParam(outputData)}");
                }

                builder.Append($@",
                    {outputValueParam(outputData)}");
            }
            builder.Append($@"
                    );

                var startResponse = new {StartResponse.TypeFullName(requestAwaiter)}
                {{
                    {StartResponse.ResponseProcess()} = topicResponse
                }};

                if(!{_responseAwaiters()}.TryAdd(topicResponse.Guid, topicResponse))
                {{
                    topicResponse.Dispose();
                    throw new InvalidOperationException();
                }}

                await {_channel()}.Writer.WriteAsync(startResponse).ConfigureAwait(false);
                await startResponse.{StartResponse.Inited()}.Task.ConfigureAwait(false);
                return 
                    new {DelayProduce.TypeFullName(requestAwaiter)}(
                        topicResponse,
                        output
                        );
            }}             
");
        }

        private static void Produce(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            string outputKeyParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Key";
            }

            string outputValueParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Value";
            }

            builder.Append($@"
            public async Task<{Response.TypeFullName(requestAwaiter)}> Produce(");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                {outputData.KeyType.GetFullTypeName(true)} {outputKeyParam(outputData)},");
                }

                builder.Append($@"
                {outputData.ValueType.GetFullTypeName(true)} {outputValueParam(outputData)}");
            }
            builder.Append($@",
                int waitResponseTimeout = 0
                )
            {{
                var guid = Guid.NewGuid().ToString(""D"");");
            CreateTopicResponse(
                builder,
                "                ",
                @"guid",
                requestAwaiter
                );
            builder.Append($@"
                topicResponse.{TopicResponse.OutputTask()}.SetResult(CreateOutput(
                    guid");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@",
                    {outputKeyParam(outputData)}");
                }

                builder.Append($@",
                    {outputValueParam(outputData)}");
            }
            builder.Append($@"
                    )
                );

                var startResponse = new {StartResponse.TypeFullName(requestAwaiter)}
                {{
                    {StartResponse.ResponseProcess()} = topicResponse
                }};

                if(!{_responseAwaiters()}.TryAdd(topicResponse.Guid, topicResponse))
                {{
                    topicResponse.Dispose();
                    throw new InvalidOperationException();
                }}

                await {_channel()}.Writer.WriteAsync(startResponse).ConfigureAwait(false);
                await startResponse.{StartResponse.Inited()}.Task.ConfigureAwait(false);
                return 
                    await topicResponse.GetResponse().ConfigureAwait(false);
            }}             
");
        }

        private static void AddAwaiter(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public {TopicResponse.TypeFullName(requestAwaiter)} AddAwaiter(
                string guid,
                int bucket,
                int waitResponseTimeout = 0
                )
            {{
                var newMessage = new KafkaExchanger.MessageInfo({InputsSize(requestAwaiter)});
                {_storage()}.Push(bucket, guid, newMessage);");
            CreateTopicResponse(
                builder,
                "                ",
                "guid",
                requestAwaiter
                );
            builder.Append($@"
                if(!{_responseAwaiters()}.TryAdd(topicResponse.Guid, topicResponse))
                {{
                    topicResponse.Dispose();
                    throw new InvalidOperationException();
                }}
                topicResponse.Init(true);
                return topicResponse;
            }}
");
        }

        private static void CreateTopicResponse(
            StringBuilder builder,
            string tabs,
            string guid,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
{tabs}var topicResponse = new {TopicResponse.TypeFullName(requestAwaiter)}(
{tabs}  {guid},
{tabs}  RemoveAwaiter,
{tabs}  {_channel()}.Writer,");
            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
{tabs}  {_currentState()},");
            }
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
{tabs}  {InputTopicPartitions(inputData)},");
            }
            if (requestAwaiter.AddAwaiterCheckStatus)
            {
                builder.Append($@"
{tabs}  CreateOutputHeader,");
            }
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
{tabs}  {SendOutput(outputData)},");

                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
{tabs}  {_afterSendOutput(outputData)},");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                        builder.Append($@"
{tabs}  {_loadOutputMessage(outputData)},
{tabs}  {_checkOutputStatus(outputData)},");
                }
            }
            builder.Append($@"
{tabs}  waitResponseTimeout
{tabs}  );");
        }

        public static string SendFuncType(KafkaExchanger.Datas.RequestAwaiter requestAwaiter, string assemblyName, OutputData outputData)
        {
            return $"Func<{OutputMessages.TypeFullName(requestAwaiter, outputData)}, {assemblyName}.RequestHeader, Task>";
        }

        public static string SendOutput(OutputData outputData)
        {
            return $"Send{outputData.NamePascalCase}";
        }

        private static void Send(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            private async Task {SendOutput(outputData)}(
                {OutputMessages.TypeFullName(requestAwaiter, outputData)} message,
                {assemblyName}.RequestHeader header
                )
            {{
                message.{OutputMessages.Message()}.Headers = new Headers
                {{
                    {{ ""Info"", header.ToByteArray() }}
                }};

                var producer = {_outputPool(outputData)}.Rent();
                try
                {{
                    var deliveryResult = await producer.ProduceAsync(
                        {_outputTopicName(outputData)},
                        message.{OutputMessages.Message()}
                        ).ConfigureAwait(false)
                        ;
                }}
                finally
                {{
                    {_outputPool(outputData)}.Return(producer);
                }}
            }}
");
            }
        }

        private static void RemoveAwaiter(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void RemoveAwaiter(string guid)
            {{
                if ({_responseAwaiters()}.TryRemove(guid, out var awaiter))
                {{
                    awaiter.Dispose();
                }}
            }}
");
        }

        public static string CreateOutputHeaderFuncType(
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter,
            string assemblyName)
        {
            return $"Func<string, {assemblyName}.RequestHeader>";
        }

        private static void CreateOutputHeader(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private {assemblyName}.RequestHeader CreateOutputHeader(string guid)
            {{
                var header = new {assemblyName}.RequestHeader()
                {{
                    MessageGuid = guid
                }};");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                var variable = i == 0 ? $"var topic" : $"topic";
                builder.Append($@"
                {variable} = new {assemblyName}.Topic()
                {{
                    Name = {_inputTopicName(inputData)}
                }};
                topic.Partitions.Add({InputTopicPartitions(inputData)});");

                if (!inputData.AcceptFromAny)
                {
                    builder.Append($@"
                topic.CanAnswerFrom.Add(new string[]{{");
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        if (j != 0)
                        {
                            builder.Append(',');
                        }

                        builder.Append('"');
                        builder.Append(inputData.AcceptedService[j]);
                        builder.Append('"');
                    }
                    builder.Append($@"}});");
                }
                builder.Append($@"
                header.TopicsForAnswer.Add(topic);");

            }
            builder.Append($@"
                return header;
            }}
");
        }

        private static void CreateOutput(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            string outputKeyParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Key";
            }

            string outputValueParam(OutputData outputData)
            {
                return $@"{outputData.NameCamelCase}Value";
            }

            builder.Append($@"
            private {OutputMessage.TypeFullName(requestAwaiter)} CreateOutput(
                string guid");

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if(!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@",
                {outputData.KeyType.GetFullTypeName(true)} {outputKeyParam(outputData)}");
                }

                builder.Append($@",
                {outputData.ValueType.GetFullTypeName(true)} {outputValueParam(outputData)}");
            }
            builder.Append($@"
                )
            {{
                var output = new {OutputMessage.TypeFullName(requestAwaiter)}
                {{");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                    {OutputMessage.Header(outputData)} = CreateOutputHeader(guid),
                    {OutputMessage.Message(outputData)} = new {OutputMessages.TypeFullName(requestAwaiter, outputData)}(
                        new Confluent.Kafka.Message<{outputData.TypesPair}>
                        {{");
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                        Key = {outputKeyParam(outputData)}{(outputData.KeyType.IsProtobuffType() ? $".ToByteArray()" : string.Empty)},");
                }
                builder.Append($@"
                        Value = {outputValueParam(outputData)}{(outputData.ValueType.IsProtobuffType() ? $".ToByteArray()" : string.Empty)}
                        }}");

                if (outputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@",
                        {outputKeyParam(outputData)}
");
                }

                if (outputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@",
                        {outputValueParam(outputData)}
");
                }
                builder.Append($@"
                        )");
            }
            builder.Append($@"
                }}
                ;");

            builder.Append($@"
                return output;
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