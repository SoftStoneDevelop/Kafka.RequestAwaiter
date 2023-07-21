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
            PrivateFilds(sb, requestAwaiter);
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
            public ConcurrentDictionary<string, {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse> _responseAwaiters = new();
            {(requestAwaiter.Data.UseLogger ? @"private readonly ILogger _logger;" : "")}

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
                _outcomeTopic{i}Name = outcomeTopic{i}Name;
                _producerPool{i} = producerPool{i};
");
            }

            builder.Append($@"
            }}
");
        }

        private static void PrivateFilds(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                private readonly string _incomeTopic{i}Name;
                private readonly int[] _incomeTopic{i}Partitions;
                private readonly string[] _incomeTopic{i}CanAnswerService;
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@"
                private readonly string _outcomeTopic{i}Name;
                private readonly {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} _producerPool{i};
");
            }
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
                _consumeRoutines[{i}] = StartTopic{i}Consume(bootstrapServers, groupId);
");
            }
            builder.Append($@"
            }}
");
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

                    consumer.Assign(_incomeTopic{i}Partitions.Select(sel => new TopicPartition(_incomeTopic{i}Name, sel)));

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

                                if (!_responseAwaiters.TryRemove(incomeMessage.HeaderInfo.AnswerToMessageGuid, out var awaiter))
                                {{
                                    {LogIncomeMessage(requestAwaiter, incomeData, "LogError", ": no one wait results")}
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                awaiter.TrySetResponse({i},incomeMessage);

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
            temp.Append($@"_logger.{logMethod}($""Consumed incomeMessage Key: ");
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
                int waitResponseTimeout = 0
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
                    new {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                        _incomeTopic{i}Name,
");
            }
            builder.Append($@"
                        header.MessageGuid,
                        RemoveAwaiter, 
                        waitResponseTimeout
                        );
                if (!_responseAwaiters.TryAdd(header.MessageGuid, awaiter))
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
                    _responseAwaiters.TryRemove(header.MessageGuid, out _);
                    awaiter.Dispose();

                    throw;
                }}
                finally
                {{
                    _producerPool{i}.Return(producer);
                }}
");
            }
            builder.Append($@"
                return await awaiter.GetResponse();
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
                    Key = {(outcomeData.KeyType.IsProtobuffType() ? $"key{messageNum}.ToByteArray()" : $"key{messageNum}")},
");
            }

            builder.Append($@"
                    Value = {(outcomeData.ValueType.IsProtobuffType() ? $"value{messageNum}.ToByteArray()" : $"value{messageNum}")}
                }};
");
        }

        private static void RemoveAwaiter(StringBuilder builder)
        {
            builder.Append($@"
            private void RemoveAwaiter(string guid)
            {{
                if (_responseAwaiters.TryRemove(guid, out var value))
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
                }};
                topic.Partitions.Add(_incomeTopic{i}Partitions);
                topic.CanAnswerFrom.Add(_incomeTopic{i}CanAnswerService);
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