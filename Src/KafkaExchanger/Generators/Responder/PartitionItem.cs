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

            Constructors(builder, responder);
            Fields(builder, responder);
            Start(builder, responder);
            StartConsume(builder, responder);
            StartHorizonRoutine(builder, responder);
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

        private static string _commitAtLeastAfter()
        {
            return "_commitAtLeastAfter";
        }

        private static string _createAnswer()
        {
            return "_createAnswer";
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

        public static void StartClass(
            StringBuilder builder
            )
        {
            builder.Append($@"
        private class {TypeName()}
        {{
" );
        }

        public static void Constructors(
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
            var commitAtLeastAfterParam = "commitAtLeastAfter";
            builder.Append($@"
            private {TypeName()}()
            {{
            }}

            public {TypeName()}(
                string {serviceNameParam},
                int {commitAtLeastAfterParam},
");
            var createAnswerParam = "createAnswer";
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
            builder.Append($@"
                {responder.CreateAnswerFuncType()} {createAnswerParam}
                )
            {{
                {_serviceName()} = {serviceNameParam};
                {_commitAtLeastAfter()} = {commitAtLeastAfterParam};
                {_createAnswer()} = {createAnswerParam};
");
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

        private static string _horizonId()
        {
            return $"_horizonId";
        }

        private static string _needCommit()
        {
            return $"_needCommit";
        }

        private static string _horizonCommitInfo()
        {
            return $"_horizonCommitInfo";
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

        public static void Fields(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private long {_horizonId()};
            private int {_needCommit()};
            private KafkaExchanger.HorizonInfo {_horizonCommitInfo()} = new(-1);
            private int {_commitAtLeastAfter()};
            private System.Threading.Tasks.TaskCompletionSource {_tcsCommit()} = new();
            private System.Threading.CancellationTokenSource {_cts()};
            private System.Threading.Thread[] {_consumeRoutines()};
            private System.Threading.Tasks.Task {_horizonRoutine()};
            
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
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                private readonly string {_inputTopicName(inputData)};
                private readonly int[] {_inputPartitions(inputData)};
");
            }
            for (int i = 0; i < responder.OutputDatas.Count; i++)
            {
                var outputData = responder.OutputDatas[i];
                builder.Append($@"
                private readonly {outputData.FullPoolInterfaceName} {_outputPool(outputData)};
");
            }
        }

        public static void Start(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public async ValueTask Start(
                string bootstrapServers,
                string groupId,
                {responder.LoadCurrentHorizonFuncType()} loadCurrentHorizon
                )
            {{
                _horizonId = await loadCurrentHorizon(
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }
                builder.Append($@"
                    {_inputPartitions(inputData)}
");
            }
            builder.Append($@"
                    );
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
                _horizonRoutine = Task.Factory.StartNew(async () => 
                {{
                    var reader = {_channel()}.Reader;
                    var storage = new KafkaExchanger.HorizonStorage();
                    try
                    {{
                        while (!{_cts()}.Token.IsCancellationRequested)
                        {{
                            var info = await reader.ReadAsync({_cts()}.Token).ConfigureAwait(false);
                            if (info is {StartResponse.TypeFullName(responder)} startResponse)
                            {{
                                storage.Add(new KafkaExchanger.HorizonInfo(startResponse.{ChannelInfo.HorizonId()}));
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
                                storage.Finish(endResponse.{ChannelInfo.HorizonId()}, offsets);

                                if (storage.CanFree() < {_commitAtLeastAfter()})
                                {{
                                    continue;
                                }}

                                var seniorСleared = storage.ClearFinished();
                                var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                                Interlocked.Exchange(ref {_horizonCommitInfo()}, seniorСleared);
                                Interlocked.Exchange(ref {_tcsCommit()}, commit);
                                Interlocked.Exchange(ref {_needCommit()}, 1);

                                await commit.Task.ConfigureAwait(false);
                            }}
                            else
                            {{
                                //TODO log error
                                //throw new Exception(""Unknown info type"");
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
                    catch (Exception ex)
                    {{
                        //TODO log
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
                builder.Append($@"
            private Thread StartConsume{inputData.NamePascalCase}(
                string bootstrapServers,
                string groupId
                )
            {{
                return new Thread((param) =>
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

                                var needCommit = Interlocked.CompareExchange(ref {_needCommit()}, 0, 1);
                                if (needCommit == 1)
                                {{
                                    var info = Volatile.Read(ref {_horizonCommitInfo()});
                                    if(info.HorizonId == -1)
                                    {{
                                        throw new Exception(""Concurrency error"");
                                    }}

                                    consumer.Commit(info.TopicPartitionOffset);

                                    //after commit delegate

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

                                var responseProcess = {_responseProcesses()}.GetOrAdd(
                                    inputMessage.{InputMessages.Header()}.MessageGuid, 
                                    (key) => 
                                    {{
                                        var horizonId = Interlocked.Increment(ref {_horizonId()});
                                        return new {ResponseProcess.TypeFullName(responder)}(
                                            key,
                                            horizonId,
                                            {_createAnswer()}, 
                                            Produce, 
                                            (key) => {_responseProcesses()}.TryRemove(key, out _),
                                            {_channel()}.Writer
                                            ); 
                                    }}
                                    );
                                
                                var startResponse = new {StartResponse.TypeFullName(responder)}()
                                {{
                                    {ChannelInfo.HorizonId()} = responseProcess.{ResponseProcess.HorizonId()}
                                }};
                                {_channel()}.Writer.WriteAsync(startResponse).GetAwaiter().GetResult();
                                responseProcess.TrySetResponse({inputData.Id}, inputMessage);
                            }}
                            catch (ConsumeException)
                            {{
                                //ignore
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
                )
                {{
                    IsBackground = true,
                    Priority = ThreadPriority.AboveNormal,
                    Name = $""{responder.TypeSymbol.Name}{{groupId}}{_inputTopicName(inputData)}""
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
                            var topicPartition = new TopicPartition(topicForAnswer.Name, topicForAnswer.Partitions.First());
                            var producer = {_outputPool(outputData)}.Rent();
                            try
                            {{
                                var deliveryResult = await producer.ProduceAsync(topicPartition, message);
                            }}
                            catch (ProduceException<{outputData.TypesPair}> e)
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
                        await Task.Delay(50);
                    }}
                }}

                {_tcsCommit()}.TrySetCanceled();
                {_channel()}.Writer.Complete();
                await {_horizonRoutine()};

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
            public async Task Stop()
            {{
                await StopConsume();
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