using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Reflection;
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
            Constructor(sb, assemblyName, requestAwaiter);
            Start(sb, requestAwaiter);

            StartConsumePartitionItem(sb, assemblyName, requestAwaiter);
            StopConsumePartitionItem(sb);

            StopPartitionItem(sb);
            ProducePartitionItem(sb, assemblyName, requestAwaiter);
            RemoveAwaiter(sb);
            CreateOutcomeHeader(sb, assemblyName, requestAwaiter);

            End(sb);
        }

        private static void StartClassPartitionItem(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        private class PartitionItem
        {{
            public ConcurrentDictionary<string, {assemblyName}.TopicResponse> _responceAwaiters = new();
            {(requestAwaiter.Data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _outcomeTopicName;

            {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? $@"private readonly Func<Task<{assemblyName}.ResponseHeader>> _createOutcomeHeader;" : "")}
            {(requestAwaiter.Data.ProducerData.CustomHeaders ? @"private readonly Func<Headers, Task> _setHeaders;" : "")}

            private CancellationTokenSource _ctsConsume;
            private Task[] _consumeRoutines;
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
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                string incomeTopic{i}Name,
                int[] incomeTopic{i}Partitions,
                string[] incomeTopic{i}CanAnswerService,
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@"
                string outcomeTopic{i}Name,
                {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }

            builder.Append($@"
                {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? $@",Func<Task<{assemblyName}.ResponseHeader>> createOutcomeHeader" : "")}
                {(requestAwaiter.Data.ProducerData.CustomHeaders ? @",Func<Headers, Task> setHeaders" : "")}
                )
            {{
                {(requestAwaiter.Data.UseLogger ? @"_logger = logger;" : "")}
                {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? @"_createOutcomeHeader = createOutcomeHeader;" : "")}
                {(requestAwaiter.Data.ProducerData.CustomHeaders ? @"_setHeaders = setHeaders;" : "")}
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                _incomeTopic{i}Name = incomeTopic{i}Name;
                _incomeTopic{i}Partitions = incomeTopic{i}Partitions;
                _incomeTopic{i}CanAnswerService = incomeTopic{i}CanAnswerService;
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@"
                _outcomeTopic{i}Name = _outcomeTopic{i}Name;
                _producerPool{i} = producerPool{i};
");
            }

            builder.Append($@"
            }}
");
        }

        private static void Start(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void Start(
                string bootstrapServers,
                string groupId
                )
            {{
                _consumeRoutines = new Task[{requestAwaiter.IncomeDatas.Count}];
                _ctsConsume = new CancellationTokenSource();
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                _consumeRoutines[{i}] = StartTopic{i}Consume(bootstrapServers, groupId, _incomeTopic{i}Partitions, _incomeTopic{i}Name);
");
            }
        }

        private static void StartConsumePartitionItem(
            StringBuilder builder, 
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            for ( int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                var incomeData = requestAwaiter.IncomeDatas[i];
                builder.Append($@"
            private Task StartTopic{i}Consume(
                string bootstrapServers,
                string groupId
                )
            {{
                return Task.Factory.StartNew(async () =>
                {{
                    var conf = new Confluent.Kafka.ConsumerConfig
                    {{
                        GroupId = groupId,
                        BootstrapServers = bootstrapServers,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        AllowAutoCreateTopics = false,
                        EnableAutoCommit = false
                    }};

                    var consumer =
                        new ConsumerBuilder<{GetConsumerTType(incomeData)}>(conf)
                        .Build()
                        ;

                    consumer.Assign(Partitions.Select(sel => new TopicPartition(_incomeTopic{i}Name, sel)));

                    try
                    {{
                        while (!_ctsConsume.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_ctsConsume.Token);
                                
                                var incomeMessage = new {requestAwaiter.Data.TypeSymbol.Name}.ResponseTopic{i}Message()
                                {{
                                    OriginalMessage = consumeResult.Message,
                                    {(incomeData.KeyType.IsKafkaNull() ? "" : $"Key = {GetResponseKey(incomeData)},")}
                                    Value = {GetResponseValue(requestAwaiter)},
                                    Partition = consumeResult.Partition
                                }};

                                {LogIncomeMessage(requestAwaiter, incomeData, "LogInformation")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {LogIncomeMessage(requestAwaiter, incomeData, "LogError")}
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = {assemblyName}.ResponseHeader.Parser.ParseFrom(infoBytes);

                                if (!_responceAwaiters.TryRemove(incomeMessage.HeaderInfo.AnswerToMessageGuid, out var awaiter))
                                {{
                                    {LogIncomeMessage(requestAwaiter, incomeData, "LogError", ": no one wait results")}
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                awaiter.TrySetResponce({i},incomeMessage);

                                bool isProcessed = false;
                                try
                                {{
                                    isProcessed = await awaiter.GetProcessStatus();
                                }}
                                catch (OperationCanceledException)
                                {{
                                    isProcessed = true;
                                    //ignore
                                }}
                                finally
                                {{
                                    awaiter.Dispose();
                                }}

                                if (!isProcessed)
                                {{
                                    {(requestAwaiter.Data.UseLogger ? @"_logger.LogWarning(""Message must be marked as processed, probably not called FinishProcessing"");" : "")}
                                }}

                                consumer.Commit(consumeResult);
                            }}
                            catch (ConsumeException e)
                            {{
                                {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "//ignore")}
                            }}
                        }}
                    }}
                    catch (OperationCanceledException)
                    {{
                        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                        consumer.Close();
                    }}
                    finally
                    {{
                        consumer.Dispose();
                    }}
                }},
            _ctsConsume.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
            );
            }}
");
            }
        }

        private static string LogIncomeMessage(
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter,
            IncomeData incomeData,
            string logMethod,
            string afterMessageInfo = ""
            )
        {
            if(!requestAwaiter.Data.UseLogger)
            {
                return string.Empty;
            }

            var temp = new StringBuilder(170);
            temp.Append($@"_logger.{logMethod}LogInformation($""Consumed incomeMessage Key: ");
            if (incomeData.KeyType.IsProtobuffType())
            {
                temp.Append(@"'{incomeMessage.Key}'");
            }
            else
            {
                temp.Append(@"'{consumeResult.Message.Key}'");
            }
            temp.Append($@", Value: ");
            if (incomeData.ValueType.IsProtobuffType())
            {
                temp.Append(@"'{incomeMessage.Value}'");
            }
            else
            {
                temp.Append(@"'{consumeResult.Message.Value}'");
            }
            temp.Append($@"{afterMessageInfo}."");");

            return temp.ToString();
        }

        private static string GetResponseKey(IncomeData incomeData)
        {
            if (incomeData.KeyType.IsProtobuffType())
            {
                return $"{incomeData.KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private static string GetResponseValue(KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter)
        {
            if (requestAwaiter.IncomeDatas[0].ValueType.IsProtobuffType())
            {
                return $"{requestAwaiter.IncomeDatas[0].ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private static void StopConsumePartitionItem(StringBuilder builder)
        {
            builder.Append($@"
            private async Task StopConsume()
            {{
                _ctsConsume?.Cancel();
                foreach (var consumeRoutine in _consumeRoutines)
                {{
                    await consumeRoutine;
                }}

                _ctsConsume?.Dispose();
            }}
");
        }

        private static void StopPartitionItem(StringBuilder builder)
        {
            builder.Append($@"
            public async Task Stop()
            {{
                await StopConsume();
            }}
");
        }

        private static void ProducePartitionItem(
            StringBuilder builder, 
            string assemblyName, 
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async Task<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                if (!requestAwaiter.OutcomeDatas[i].KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                {requestAwaiter.OutcomeDatas[i].KeyType.GetFullTypeName(true)} key{i},
");
                }

                builder.Append($@"
                {requestAwaiter.OutcomeDatas[i].ValueType.GetFullTypeName(true)} value{i},
");
            }
            builder.Append($@"
                int waitResponceTimeout = 0
                )
            {{
                var messageGuid = Guid.NewGuid().ToString(""D"");
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeData = requestAwaiter.OutcomeDatas[i];
                CreateOutcomeMessage(builder, requestAwaiter, outcomeData, i);
                var headerVariable = i == 0 ? "var header" : "header";
                if(requestAwaiter.Data.ProducerData.CustomOutcomeHeader)
                {
                    builder.Append($@"
                {headerVariable} = await _createOutcomeHeader();
");
                }
                else
                {
                    builder.Append($@"
                {headerVariable} = CreateOutcomeHeader();
");
                }

                builder.Append($@"
                header.MessageGuid = messageGuid;
                message{i}.Headers = new Headers
                {{
                    {{ ""Info"", header.ToByteArray() }}
                }};

                {(requestAwaiter.Data.ProducerData.CustomHeaders ? $"await _setHeaders(message{i}.Headers);" : "")}
");
            }
            builder.Append($@"
                var awaiter = 
                    new TestProtobuffAwaiter.TopicResponse(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                        _incomeTopic{i}Name,
");
            }
            builder.Append($@"
                        header.MessageGuid.
                        RemoveAwaiter, 
                        waitResponceTimeout
                        );
                if (!_responceAwaiters.TryAdd(header.MessageGuid, awaiter))
                {{
                    awaiter.Dispose();
                    throw new Exception();
                }}
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeData = requestAwaiter.OutcomeDatas[i];
                var variable = i == 0 ? "var producer" : "producer";
                builder.Append($@"
                {variable} = _producerPool{i}.Rent();
                try
                {{
                    var deliveryResult = await producer.ProduceAsync(_outcomeTopic{i}Name, message{i});
                }}
                catch (ProduceException<{outcomeData.TypesPair}> e)
                {{
                    _logger.LogError($""Delivery failed: {{e.Error.Reason}}"");
                    _responceAwaiters.TryRemove(header.MessageGuid, out _);
                    awaiter.Dispose();

                    throw;
                }}
                finally
                {{
                    _producerPool.Return(producer);
                }}
");
            }
            builder.Append($@"
                return await awaiter.GetResponce();
            }}
");
        }

        private static void CreateOutcomeMessage(
            StringBuilder builder, 
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter,
            OutcomeData outcomeData,
            int messageNum
            )
        {
            builder.Append($@"
                var message{messageNum} = new Message<{outcomeData.TypesPair}>()
                {{
");

            if (!outcomeData.KeyType.IsKafkaNull())
            {
                builder.Append($@"
                    Key = {(outcomeData.KeyType.IsProtobuffType() ? "key.ToByteArray()" : "key")},
");
            }

            builder.Append($@"
                    Value = {(outcomeData.ValueType.IsProtobuffType() ? "value.ToByteArray()" : "value")}
                }};
");
        }

        private static void RemoveAwaiter(StringBuilder builder)
        {
            builder.Append($@"
            private void RemoveAwaiter(string guid)
            {{
                if (_responceAwaiters.TryRemove(guid, out var value))
                {{
                    value.Dispose();
                }}
            }}
");
        }

        private static void CreateOutcomeHeader(
            StringBuilder builder,
            string assemblyName, 
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            if (requestAwaiter.Data.ProducerData.CustomOutcomeHeader)
            {
                //nothing
            }
            else
            {
                builder.Append($@"
            private {assemblyName}.RequestHeader CreateOutcomeHeader()
            {{
                var headerInfo = new {assemblyName}.RequestHeader();
");
                for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
                {
                    var variable = i == 0 ? $"var topic" : $"topic";
                    builder.Append($@"
                {variable} = new {assemblyName}.Topic()
                {{
                    Name = _incomeTopic{i}Name
                }}
                topic.Partitions.Add(_incomeTopic{i}Partitions);
                topic.CanAnswerFrom.Add(_incomeTopic{i}CanAswerService);
                headerInfo.TopicsForAnswer.Add(topic);
");

                }
                builder.Append($@"
                return headerInfo;
            }}
");
            }
        }

        private static string GetConsumerTType(IncomeData incomeData)
        {
            return $@"{(incomeData.KeyType.IsProtobuffType() ? "byte[]" : incomeData.KeyType.GetFullTypeName(true))}, {(incomeData.ValueType.IsProtobuffType() ? "byte[]" : incomeData.ValueType.GetFullTypeName(true))}";
        }

        private static void End(StringBuilder builder)
        {
            builder.Append($@"
        }}
");
        }
    }
}