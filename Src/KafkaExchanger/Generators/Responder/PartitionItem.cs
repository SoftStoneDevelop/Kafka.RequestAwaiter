using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class PartitionItem
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            StartClass(builder);

            Constructor(builder, responder);
            Fields(builder, responder);
            Start(builder, responder);
            StartConsume(builder, responder);
            StartHorizonRoutine(builder, responder);
            StartInitializeRoutine(builder, responder);
            StartConsumeInput(builder, assemblyName, responder);
            Produce(builder, responder);
            CreateOutputHeader(builder, assemblyName, responder);

            Stop(builder, responder);
            StopConsume(builder, responder);

            EndClass(builder);
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "PartitionItem";
        }

        private static string _serviceName()
        {
            return "_serviceName";
        }

        private static string _createAnswer()
        {
            return "_createAnswer";
        }

        private static string _maxBuckets()
        {
            return "_maxBuckets";
        }

        private static string _itemsInBucket()
        {
            return "_itemsInBucket";
        }

        private static string _addNewBucket()
        {
            return "_addNewBucket";
        }

        private static string _inputTopicName(InputData inputData)
        {
            return $"_{inputData.NameCamelCase}Name";
        }

        private static string _inputPartitions(InputData inputData)
        {
            return $"_{inputData.NameCamelCase}Partitions";
        }

        private static string _outputPool(OutputData outputData)
        {
            return $"_{outputData.NameCamelCase}Pool";
        }

        private static string _logger()
        {
            return "_logger";
        }

        private static string _afterCommit()
        {
            return "_afterCommit";
        }

        private static string _afterSend()
        {
            return "_afterSend";
        }

        private static string _checkState()
        {
            return "_checkState";
        }

        public static void StartClass(
            StringBuilder builder
            )
        {
            builder.Append($@"
        private class {TypeName()}
        {{
" );
        }

        public static void Constructor(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            string inputTopicName(InputData inputData)
            {
                return $"{inputData.NameCamelCase}Name";
            }

            string inputPartitions(InputData inputData)
            {
                return $"{inputData.NameCamelCase}Partitions";
            }

            string outputPool(OutputData outputData)
            {
                return $"{outputData.NameCamelCase}Pool";
            }

            var serviceNameParam = "serviceName";
            var maxBucketsParam = "maxBuckets";
            var itemsInBucketParam = "itemsInBucket";
            var addNewBucketParam = "addNewBucket";


            builder.Append($@"
            private {TypeName()}() {{ }}

            public {TypeName()}(
                string {serviceNameParam},
                int {maxBucketsParam},
                int {itemsInBucketParam},
                {responder.AddNewBucketFuncType()} {addNewBucketParam},
");
            var loggerParametr = "logger";
            if(responder.UseLogger)
            {
                builder.Append($@"
                ILogger {loggerParametr},");
            }

            var afterCommitParam = "afterCommit";
            if (responder.AfterCommit)
            {
                builder.Append($@"
                {responder.AfterCommitFuncType()} {afterCommitParam},");
            }

            var checkStateParam = "checkState";
            if (responder.CheckCurrentState)
            {
                builder.Append($@"
                {responder.CheckCurrentStateFuncType()} {checkStateParam},");
            }

            var afterSendParam = "afterSend";
            if (responder.AfterSend)
            {
                builder.Append($@"
                {responder.AfterSendFuncType()} {afterSendParam},");
            }

            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                string {inputTopicName(inputData)},
                int[] {inputPartitions(inputData)},
");
            }
            for (int i = 0; i < responder.OutputDatas.Count; i++)
            {
                var outputData = responder.OutputDatas[i];
                builder.Append($@"
                {outputData.FullPoolInterfaceName} {outputPool(outputData)},
");
            }

            var createAnswerParam = "createAnswer";
            builder.Append($@"
                {responder.CreateAnswerFuncType()} {createAnswerParam}
                )
            {{
                {_serviceName()} = {serviceNameParam};
                {_maxBuckets()} = {maxBucketsParam};
                {_itemsInBucket()} = {itemsInBucketParam};
                {_addNewBucket()} = {addNewBucketParam};
                {_createAnswer()} = {createAnswerParam};");
            
            if (responder.UseLogger)
            {
                builder.Append($@"
                {_logger()} = {loggerParametr};");
            }

            if (responder.AfterCommit)
            {
                builder.Append($@"
                {_afterCommit()} = {afterCommitParam};");
            }

            if (responder.CheckCurrentState)
            {
                builder.Append($@"
                {_checkState()} = {checkStateParam};");
            }

            if (responder.AfterSend)
            {
                builder.Append($@"
                {_afterSend()} = {afterSendParam};");
            }

            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                {_inputTopicName(inputData)} = {inputTopicName(inputData)};
                {_inputPartitions(inputData)} = {inputPartitions(inputData)};
");
            }
            for (int i = 0; i < responder.OutputDatas.Count; i++)
            {
                var outputData = responder.OutputDatas[i];
                builder.Append($@"
                {_outputPool(outputData)} = {outputPool(outputData)};
");
            }
            builder.Append($@"
            }}
");
        }

        private static string _needCommit()
        {
            return $"_needCommit";
        }

        private static string _commitOffsets()
        {
            return $"_commitOffsets";
        }

        private static string _tcsCommit()
        {
            return $"_tcsCommit";
        }

        private static string _responseProcesses()
        {
            return $"_responseProcesses";
        }

        private static string _channel()
        {
            return $"_channel";
        }

        private static string _initializeChannel()
        {
            return $"_initializeChannel";
        }

        private static string _cts()
        {
            return $"_cts";
        }

        private static string _consumeRoutines()
        {
            return $"_consumeRoutines";
        }

        private static string _horizonRoutine()
        {
            return $"_horizonRoutine";
        }

        private static string _initializeRoutine()
        {
            return $"_initializeRoutine";
        }

        public static void Fields(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private int {_needCommit()};
            private Confluent.Kafka.TopicPartitionOffset[] {_commitOffsets()};
            private System.Threading.Tasks.TaskCompletionSource {_tcsCommit()} = new();
            private System.Threading.CancellationTokenSource {_cts()};
            private System.Threading.Thread[] {_consumeRoutines()};
            private System.Threading.Tasks.Task {_horizonRoutine()};
            private System.Threading.Tasks.Task {_initializeRoutine()};
            
            private readonly int {_maxBuckets()};
            private readonly int {_itemsInBucket()};
            private readonly {responder.AddNewBucketFuncType()} {_addNewBucket()};
            private readonly {responder.CreateAnswerFuncType()} {_createAnswer()};
            private readonly string {_serviceName()};
            private readonly ConcurrentDictionary<string, {ResponseProcess.TypeFullName(responder)}> {_responseProcesses()} = new();
            private readonly Channel<{ChannelInfo.TypeFullName(responder)}> {_channel()} = Channel.CreateUnbounded<{ChannelInfo.TypeFullName(responder)}>(
                new UnboundedChannelOptions() 
                {{
                    AllowSynchronousContinuations = false, 
                    SingleReader = true,
                    SingleWriter = false
                }});

            private readonly Channel<{ResponseProcess.TypeFullName(responder)}> {_initializeChannel()} = Channel.CreateUnbounded<{ResponseProcess.TypeFullName(responder)}>(
                new UnboundedChannelOptions() 
                {{
                    AllowSynchronousContinuations = false, 
                    SingleReader = true,
                    SingleWriter = true
                }});
");

            if(responder.UseLogger)
            {
                builder.Append($@"
                private readonly ILogger {_logger()};");
            }

            if (responder.AfterCommit)
            {
                builder.Append($@"
                private readonly {responder.AfterCommitFuncType()} {_afterCommit()};");
            }

            if (responder.CheckCurrentState)
            {
                builder.Append($@"
                private readonly {responder.CheckCurrentStateFuncType()} {_checkState()};");
            }

            if (responder.AfterSend)
            {
                builder.Append($@"
                private readonly {responder.AfterSendFuncType()} {_afterSend()};");
            }

            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                private readonly string {_inputTopicName(inputData)};
                private readonly int[] {_inputPartitions(inputData)};");
            }
            for (int i = 0; i < responder.OutputDatas.Count; i++)
            {
                var outputData = responder.OutputDatas[i];
                builder.Append($@"
                private readonly {outputData.FullPoolInterfaceName} {_outputPool(outputData)};");
            }
        }

        public static void Start(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public void Start(
                string bootstrapServers,
                string groupId
                )
            {{
                StartConsume(bootstrapServers, groupId);
            }}
");
        }

        public static void StartConsume(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private void StartConsume(
                string bootstrapServers,
                string groupId
                )
            {{
                {_cts()} = new CancellationTokenSource();
                StartHorizonRoutine();
                StartInitializeRoutine();
                {_consumeRoutines()} = new Thread[{responder.InputDatas.Count}];
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                if (i != 0)
                {
                    builder.Append(',');
                }
                builder.Append($@"
                    {_consumeRoutines()}[{i}] = StartConsume{inputData.NamePascalCase}(bootstrapServers, groupId);
                    {_consumeRoutines()}[{i}].Start();
");
            }
            builder.Append($@"
            }}
");
        }

        public static void StartHorizonRoutine(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private void StartHorizonRoutine()
            {{
                {_horizonRoutine()} = Task.Factory.StartNew(async () => 
                {{
                    var reader = {_channel()}.Reader;
                    var writer = {_initializeChannel()}.Writer;
                    var storage = new KafkaExchanger.BucketStorage(
                        maxBuckets: {_maxBuckets()},
                        itemsInBucket: {_itemsInBucket()},
                        addNewBucket: {_addNewBucket()}
                        );

                    await storage.Init(
                        minBuckets: 5,
                        currentBucketsCount: static async () =>
                        {{
                            return await Task.FromResult(5);
                        }}
                        );
                    try
                    {{
                        while (!{_cts()}.Token.IsCancellationRequested)
                        {{
                            var info = await reader.ReadAsync({_cts()}.Token).ConfigureAwait(false);
                            if (info is {StartResponse.TypeFullName(responder)} startResponse)
                            {{
                                var newMessage = new KafkaExchanger.MessageInfo();
                                newMessage.SetProcess(startResponse.{StartResponse.ResponseProcess()});
                                var result = await storage.Push(startResponse.{StartResponse.ResponseProcess()}.{ResponseProcess.Guid()}, newMessage);
                                if(result.NeedStart)
                                {{
                                    var process = ({ResponseProcess.TypeFullName(responder)})result.Process;
                                    process.{ResponseProcess.BucketId()} = result.BucketId;

                                    await writer.WriteAsync(process);
                                }}
                            }}
                            else if (info is {EndResponse.TypeFullName(responder)} endResponse)
                            {{
                                var offsets = new Confluent.Kafka.TopicPartitionOffset[{responder.InputDatas.Count}];
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                                offsets[{i}] = endResponse.{EndResponse.Offset(inputData)};
");
            }
            builder.Append($@"
                                storage.Finish(endResponse.{EndResponse.BucketId()}, endResponse.{EndResponse.Guid()}, offsets);

                                while (storage.TryPop(out var bucketId, out var canFreeInfos, out var needInit))
                                {{
                                    var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                                    Volatile.Write(ref {_commitOffsets()}, canFreeInfos.Values.OrderByDescending(or => or.TopicPartitionOffset[0].Offset).First().TopicPartitionOffset);
                                    Interlocked.Exchange(ref {_tcsCommit()}, commit);
                                    Interlocked.Exchange(ref {_needCommit()}, 1);

                                    await commit.Task.ConfigureAwait(false);");
            if(responder.AfterCommit)
            {
                builder.Append($@"
                                await {_afterCommit()}(
                                    bucketId");
                for (int i = 0; i < responder.InputDatas.Count; i++)
                {
                    var inputData = responder.InputDatas[i];
                    builder.Append($@",
                                    {_inputPartitions(inputData)}");
                }
                builder.Append($@"
                                    ).ConfigureAwait(false);");
            }
            builder.Append($@"
                                    if(needInit != null)
                                    {{
                                        foreach (var message in needInit.Values)
                                        {{
                                            var process = ({ResponseProcess.TypeFullName(responder)})message.TakeProcess();
                                            process.BucketId = bucketId;

                                            await writer.WriteAsync(process);
                                        }}
                                    }}
                                }}
                            }}
                            else
                            {{
                                {(responder.UseLogger ? $@"{_logger()}.LogError(""Unknown info type"");" : "//ignore")}
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
                    catch (Exception {(responder.UseLogger ? $"ex" : string.Empty)})
                    {{
                        {(responder.UseLogger ? $@"{_logger()}.LogError(ex, ""Error commit task"");" : string.Empty)}
                        throw;
                    }}
                }});
            }}
");
        }

        public static void StartInitializeRoutine(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
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
                    catch (Exception {(responder.UseLogger ? $"ex" : string.Empty)})
                    {{
                        {(responder.UseLogger ? $@"{_logger()}.LogError(ex, ""Error init task"");" : string.Empty)}
                        throw;
                    }}
                }});
            }}
");
        }

        public static void StartConsumeInput(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                var threadName = $@"{responder.TypeSymbol.Name}{{groupId}}{_inputTopicName(inputData)}";
                builder.Append($@"
            private Thread StartConsume{inputData.NamePascalCase}(
                string bootstrapServers,
                string groupId
                )
            {{
                return new Thread((param) =>
                {{
                    start:
                    if(_cts.Token.IsCancellationRequested)
                    {{
                        return;
                    }}

                    try
                    {{
                        var conf = new ConsumerConfig
                        {{
                            GroupId = groupId,
                            BootstrapServers = bootstrapServers,
                            AutoOffsetReset = AutoOffsetReset.Earliest,
                            AllowAutoCreateTopics = false,
                            EnableAutoCommit = false
                        }};

                        var consumer =
                            new ConsumerBuilder<{inputData.TypesPair}>(conf)
                            .Build()
                            ;

                        consumer.Assign({_inputPartitions(inputData)}.Select(sel => new Confluent.Kafka.TopicPartition({_inputTopicName(inputData)}, sel)));

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
                if(inputData.KeyType.IsProtobuffType())
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

                                    inputMessage.{InputMessages.Header()} = {assemblyName}.RequestHeader.Parser.ParseFrom(infoBytes);
                                    if (!inputMessage.{InputMessages.Header()}.TopicsForAnswer.Any(wh => !wh.CanAnswerFrom.Any() || wh.CanAnswerFrom.Contains({_serviceName()})))
                                    {{
                                        continue;
                                    }}

                                    {ResponseProcess.TypeFullName(responder)} responseProcess = null;
                                    var created = false;
                                    while (responseProcess == null)
                                    {{
                                        if(!{_responseProcesses()}.TryGetValue(inputMessage.{InputMessages.Header()}.MessageGuid, out responseProcess))
                                        {{
                                            responseProcess = new {ResponseProcess.TypeFullName(responder)}(
                                                inputMessage.{InputMessages.Header()}.MessageGuid,
                                                {_createAnswer()}, 
                                                Produce, 
                                                (key) => {_responseProcesses()}.TryRemove(key, out _),
                                                {_channel()}.Writer");
                var needPartitions = false;
                if (responder.AfterSend)
                {
                    needPartitions |= true;
                    builder.Append($@",
                                                {_afterSend()}");
                }

                if (responder.CheckCurrentState)
                {
                    needPartitions |= true;
                    builder.Append($@",
                                                {_checkState()}");
                }

                if (needPartitions)
                {
                    for (int j = 0; j < responder.InputDatas.Count; j++)
                    {
                        var inputDataItem = responder.InputDatas[j];
                        builder.Append($@",
                                                {_inputPartitions(inputData)}");
                    }
                }
                builder.Append($@"
                                                );
                                            if({_responseProcesses()}.TryAdd(inputMessage.{InputMessages.Header()}.MessageGuid, responseProcess))
                                            {{
                                                created = true;
                                            }}
                                            else
                                            {{
                                                responseProcess.Dispose();
                                                responseProcess = null;
                                            }}
                                        }}
                                    }}
                                    
                                    if(created)
                                    {{
                                        var startResponse = new {StartResponse.TypeFullName(responder)}()
                                        {{
                                            {StartResponse.ResponseProcess()} = responseProcess
                                        }};
                                        {_channel()}.Writer.WriteAsync(startResponse).GetAwaiter().GetResult();
                                    }}

                                    responseProcess.TrySetResponse({inputData.Id}, inputMessage);
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
                    catch (Exception {(responder.UseLogger ? $"ex" : string.Empty)})
                    {{
                        {(responder.UseLogger ? $@"{_logger()}.LogError(ex, $""{threadName}"");" : string.Empty)}
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

        public static void Produce(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private async Task Produce(
                {OutputMessage.TypeFullName(responder)} outputMessage,
                {InputMessage.TypeFullName(responder)} inputMessage
                )
            {{
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                if (inputMessage.{InputMessage.Message(inputData)}.Header.TopicsForAnswer.Any())
                {{
");
                for (int j = 0; j < responder.OutputDatas.Count; j++)
                {
                    var outputData = responder.OutputDatas[j];
                    builder.Append($@"
                    {{
                        var message = new Message<{outputData.TypesPair}>()
                        {{
");
                    if(!outputData.KeyType.IsKafkaNull())
                    {
                        if(outputData.KeyType.IsProtobuffType())
                        {
                            builder.Append($@"
                            Key = outputMessage.{OutputMessage.Message(outputData)}.{OutputMessages.Key()}.ToByteArray(),
");
                        }
                        else
                        {
                            builder.Append($@"
                            Key = outputMessage.{OutputMessage.Message(outputData)}.{OutputMessages.Key()},
");
                        }
                    }

                    if (outputData.ValueType.IsProtobuffType())
                    {
                        builder.Append($@"
                            Value = outputMessage.{OutputMessage.Message(outputData)}.{OutputMessages.Value()}.ToByteArray(),
");
                    }
                    else
                    {
                        builder.Append($@"
                            Value = outputMessage.{OutputMessage.Message(outputData)}.{OutputMessages.Value()}
");
                    }
                    builder.Append($@"
                        }};

                        var header = CreateOutputHeader(
                            inputMessage.{InputMessage.Message(inputData)}.{InputMessages.Header()}.Bucket,
                            inputMessage.{InputMessage.Message(inputData)}.{InputMessages.Header()}.MessageGuid
                            );
                        message.Headers = new Headers
                        {{
                            {{ ""Info"", header.ToByteArray() }}
                        }};

                        foreach (var topicForAnswer in 
                                                        inputMessage.
                                                        {InputMessage.Message(inputData)}.
                                                        Header.TopicsForAnswer.Where(wh => 
                                                                                        !wh.CanAnswerFrom.Any() || 
                                                                                        wh.CanAnswerFrom.Contains({_serviceName()})
                                                                                    )
                        )
                        {{
                            var index = Interlocked.Increment(ref _partitionIndex) % (uint)topicForAnswer.Partitions.Count;
                            var topicPartition = new TopicPartition(topicForAnswer.Name, topicForAnswer.Partitions[(int)index]);
                            var producer = {_outputPool(outputData)}.Rent();
                            try
                            {{
                                var deliveryResult = await producer.ProduceAsync(topicPartition, message);
                            }}
                            catch (ProduceException<{outputData.TypesPair}>)
                            {{
                                //ignore
                            }}
                            finally
                            {{
                                {_outputPool(outputData)}.Return(producer);
                            }}
                        }}
                    }}
");
                }
                builder.Append($@"
                }}
");
            }
            builder.Append($@"
            }}

            private uint _partitionIndex = 0;
");
        }

        public static void CreateOutputHeader(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private {assemblyName}.ResponseHeader CreateOutputHeader(int bucket, string answerToMessageGuid)
            {{
                var header = new {assemblyName}.ResponseHeader()
                {{
                    AnswerToMessageGuid = answerToMessageGuid,
                    AnswerFrom = {_serviceName()},
                    Bucket = bucket
                }};

                return header;
            }}
");
        }

        public static void StopConsume(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private async Task StopConsume()
            {{
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
                    await {_horizonRoutine()};
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

                {_cts()}?.Dispose();
            }}
");
        }

        public static void Stop(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public Task Stop()
            {{
                return StopConsume();
            }}
");
        }

        public static void EndClass(
            StringBuilder builder
            )
        {
            builder.Append($@"
        }}
");
        }
    }
}