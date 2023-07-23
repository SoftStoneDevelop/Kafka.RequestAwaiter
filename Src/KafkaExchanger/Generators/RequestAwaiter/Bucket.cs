using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Bucket
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
            Dispose(sb);
            Start(sb, requestAwaiter);

            StartConsumePartitionItem(sb, assemblyName, requestAwaiter);
            StopConsume(sb, requestAwaiter);

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
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.Data.ProducerData;

            builder.Append($@"
            private class Bucket : IDisposable
            {{
                private readonly int _bucketId;
                private readonly int _maxInFly;
                private ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
                private int _addedCount;
                private readonly Dictionary<string, {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse> _responseAwaiters;
                {(requestAwaiter.Data.UseLogger ? @"private readonly ILogger _logger;" : "")}
                {(consumerData.CheckCurrentState ? $"private readonly {consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} _getCurrentState;" : "")}
                {(consumerData.UseAfterCommit ? $"private readonly {consumerData.AfterCommitFunc(requestAwaiter.IncomeDatas)} _afterCommit;" : "")}
                {(producerData.AfterSendResponse ? $@"private readonly {producerData.AfterSendResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.OutcomeDatas)} _afterSendResponse;" : "")}
                {(producerData.CustomOutcomeHeader ? $@"private readonly {producerData.CustomOutcomeHeaderFunc(assemblyName)} _createOutcomeHeader;" : "")}
                {(producerData.CustomHeaders ? $@"private readonly {producerData.CustomHeadersFunc()} _setHeaders;" : "")}

                private CancellationTokenSource _ctsConsume;
                private Task[] _consumeRoutines;
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                private CancellationTokenSource _ctsConsume{i};
                private TaskCompletionSource<HashSet<Confluent.Kafka.Partition>> _tcsPartitions{i};
");
            }
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
                public Bucket(
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
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.Data.ProducerData;
            builder.Append($@",int bucketId,
                    int maxInFly
                    {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                    {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState" : "")}
                    {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.IncomeDatas)} afterCommit" : "")}
                    {(producerData.AfterSendResponse ? $@",{producerData.AfterSendResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.OutcomeDatas)} afterSendResponse" : "")}
                    {(producerData.CustomOutcomeHeader ? $@",{producerData.CustomOutcomeHeaderFunc(assemblyName)} createOutcomeHeader" : "")}
                    {(producerData.CustomHeaders ? $@",{producerData.CustomHeadersFunc()} setHeaders" : "")}
                    )
                {{
                    _bucketId = bucketId;
                    _maxInFly = maxInFly;
                    _responseAwaiters = new(_maxInFly);

                    {(requestAwaiter.Data.UseLogger ? @"_logger = logger;" : "")}
                    {(consumerData.CheckCurrentState ? $"_getCurrentState = getCurrentState;" : "")}
                    {(consumerData.UseAfterCommit ? $"_afterCommit = afterCommit;" : "")}
                    {(producerData.AfterSendResponse ? $@"_afterSendResponse = afterSendResponse;" : "")}
                    {(producerData.CustomOutcomeHeader ? $@"_createOutcomeHeader = createOutcomeHeader;" : "")}
                    {(producerData.CustomHeaders ? $@"_setHeaders = setHeaders;" : "")}
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

        private static void Dispose(
            StringBuilder builder
            )
        {
            builder.Append($@"
            public void Dispose()
            {{
                _lock.Dispose();
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
                _consumeRoutines[{i}] = StartTopic{i}Consume(bootstrapServers, groupId);
                _ctsConsume{i} = new();
                _tcsPartitions{i} = new();
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
            var consumerData = requestAwaiter.Data.ConsumerData;
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                var incomeData = requestAwaiter.IncomeDatas[i];
                builder.Append($@"
                private Task StartTopic{i}Consume(
                    string bootstrapServers,
                    string groupId
                    )
                {{
                    return Task.Factory.StartNew(() =>
                    {{
                        var conf = new Confluent.Kafka.ConsumerConfig
                        {{
                            GroupId = $""{{groupId}}Bucket{{_bucketId}}"",
                            BootstrapServers = bootstrapServers,
                            AutoOffsetReset = AutoOffsetReset.Earliest,
                            AllowAutoCreateTopics = false,
                            EnableAutoCommit = false
                        }};

                        var consumer =
                            new ConsumerBuilder<{incomeData.TypesPair}>(conf)
                            .Build()
                            ;

                        consumer.Assign(_incomeTopic{i}Partitions.Select(sel => new TopicPartition(_incomeTopic{i}Name, sel)));

                        try
                        {{
                            var offsets = new Dictionary<Partition, TopicPartitionOffset>();
                            while (!_ctsConsume.Token.IsCancellationRequested)
                            {{
                                try
                                {{
                                    _lock.EnterUpgradeableReadLock();
                                    try
                                    {{
                                        if(_addedCount == _maxInFly && _responseAwaiters.Count == 0)
                                        {{
                                            _lock.EnterWriteLock();
                                            try
                                            {{
                                                _addedCount = 0;
                                                consumer.Commit(offsets.Values);
");
                if (consumerData.UseAfterCommit)
                {
                    for (int j = 0; j < requestAwaiter.IncomeDatas.Count; j++)
                    {
                        if(j == i)
                        {
                            continue;
                        }

                        builder.Append($@"
                                                Volatile.Read(ref _ctsConsume{j}).Cancel();
                                                var tcsPartitions{j} = new TaskCompletionSource<HashSet<Confluent.Kafka.Partition>>();
                                                Volatile.Write(ref _tcsPartitions{j}, tcsPartitions{j});
                                                var partitions{j} = tcsPartitions{j}.Task.Result;
");
                    }
                    builder.Append(@"
                                                _afterCommit(
                                                    _bucketId
");
                    for (int j = 0; j < requestAwaiter.IncomeDatas.Count; j++)
                    {
                        builder.Append(',');
                        if (j == i)
                        {
                            builder.Append(@"
                                                    offsets.Keys.ToHashSet()
");
                        }
                        else
                        {
                            builder.Append($@"
                                                    partitions{j}
");
                        }
                    }
                    builder.Append(@"
                                                )
                                                .Wait();
");
                }
                builder.Append($@"
                                            }}
                                            finally
                                            {{
                                                _lock.ExitWriteLock();
                                            }}
                                        }}
                                    }}
                                    finally
                                    {{
                                        _lock.ExitUpgradeableReadLock();
                                    }}

                                    ConsumeResult<{incomeData.TypesPair}> consumeResult;
                                    while (true)
                                    {{
                                        try
                                        {{
                                            consumeResult = consumer.Consume(_ctsConsume{i}.Token);
                                            break;
                                        }}
                                        catch (OperationCanceledException)
                                        {{
                                            _ctsConsume.Token.ThrowIfCancellationRequested();
                                            _ctsConsume{i}.Dispose();
                                            _ctsConsume{i} = new CancellationTokenSource();
                                            Volatile.Read(ref _tcsPartitions{i}).SetResult(offsets.Keys.ToHashSet());
                                        }}
                                    }}
                                    offsets[consumeResult.Partition] = consumeResult.TopicPartitionOffset;
                                
                                    var incomeMessage = new {requestAwaiter.Data.TypeSymbol.Name}.Income{i}Message()
                                    {{
                                        OriginalMessage = consumeResult.Message,
                                        {(incomeData.KeyType.IsKafkaNull() ? "" : $"Key = {GetResponseKey(incomeData)},")}
                                        Value = {GetResponseValue(incomeData)},
                                        Partition = consumeResult.Partition
                                    }};

                                    {LogIncomeMessage(requestAwaiter, incomeData, "LogInformation")}
                                    if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                    {{
                                        {LogIncomeMessage(requestAwaiter, incomeData, "LogError")}
                                        continue;
                                    }}

                                    incomeMessage.HeaderInfo = {assemblyName}.ResponseHeader.Parser.ParseFrom(infoBytes);

                                    TopicResponse topicResponse;
                                    _lock.EnterReadLock();
                                    try
                                    {{
                                        if (!_responseAwaiters.TryGetValue(incomeMessage.HeaderInfo.AnswerToMessageGuid, out topicResponse))
                                        {{
                                            {LogIncomeMessage(requestAwaiter, incomeData, "LogError", ": no one wait results")}
                                            continue;
                                        }}

                                        topicResponse.TrySetResponse({i}, incomeMessage);
                                    }}
                                    finally
                                    {{
                                        _lock.ExitReadLock();
                                    }}
                                }}
                                catch (ConsumeException {(requestAwaiter.Data.UseLogger ? "e" : "")})
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

        private static void StopConsume(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
                public async Task StopConsume()
                {{
                    _ctsConsume?.Cancel();
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                CancellationTokenSource ctsConsume{i};
");
            }
            builder.Append($@"
                _lock.EnterWriteLock();
                try
                {{
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                    ctsConsume{i} = Volatile.Read(ref _ctsConsume{i});
                    ctsConsume{i}?.Cancel();
");
            }
            builder.Append($@"
                }}
                finally
                {{ 
                    _lock.ExitWriteLock(); 
                }}
");
            builder.Append($@"
                    foreach (var consumeRoutine in _consumeRoutines)
                    {{
                        await consumeRoutine;
                    }}

                    _ctsConsume?.Dispose();
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                    ctsConsume{i}?.Dispose();
");
            }
            builder.Append($@"
                }}
");
        }

        private static void ProducePartitionItem(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var producerData = requestAwaiter.Data.ProducerData;
            var consumerData = requestAwaiter.Data.ConsumerData;

            builder.Append($@"
            public async Task<{assemblyName}.TryProduceResult> TryProduce(
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
                if (producerData.CustomOutcomeHeader)
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

                {(producerData.CustomHeaders ? $"await _setHeaders(message{i}.Headers);" : "")}
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
            if(consumerData.CheckCurrentState)
            {
                builder.Append($@"
                        _getCurrentState,
");
            }
            builder.Append($@"
                        header.MessageGuid,
                        RemoveAwaiter, 
                        waitResponseTimeout
                        );
                    _lock.EnterUpgradeableReadLock();
                    try
                    {{
                        if(_responseAwaiters.Count == _maxInFly)
                        {{
                            awaiter.Dispose();
                            return new KafkaExchengerTests.TryProduceResult {{ Succsess = false }};
                        }}
                        else
                        {{
                            _lock.EnterWriteLock();
                            try
                            {{
                                if (!_responseAwaiters.TryAdd(header.MessageGuid, awaiter))
                                {{
                                    awaiter.Dispose();
                                    throw new Exception();
                                }}
                                else
                                {{
                                    _addedCount++;
                                }}
                            }}
                            finally
                            {{
                                _lock.ExitWriteLock();
                            }}
                        }}
                    }}
                    finally
                    {{
                        _lock.ExitUpgradeableReadLock();
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
                catch (ProduceException<{outcomeData.TypesPair}> {(requestAwaiter.Data.UseLogger ? "e" : "")})
                {{
                        {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
                        _lock.EnterWriteLock();
                        try
                        {{
                            _responseAwaiters.Remove(header.MessageGuid, out _);
                        }}
                        finally
                        {{
                            _lock.ExitWriteLock();
                        }}
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
                var response = await awaiter.GetResponse();
                return new KafkaExchengerTests.TryProduceResult() {{Succsess = true, Response = response}};
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
                    _lock.EnterWriteLock();
                    try
                    {{
                        if(_responseAwaiters.Remove(guid, out var value))
                        {{
                            value.Dispose();
                        }}
                    }}
                    finally
                    {{
                        _lock.ExitWriteLock();
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

        private static string LogIncomeMessage(
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter,
            IncomeData incomeData,
            string logMethod,
            string afterMessageInfo = ""
            )
        {
            if (!requestAwaiter.Data.UseLogger)
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

        private static string GetResponseValue(IncomeData incomeData)
        {
            if (incomeData.ValueType.IsProtobuffType())
            {
                return $"{incomeData.ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private static void End(StringBuilder builder)
        {
            builder.Append($@"
        }}
");
        }
    }
}