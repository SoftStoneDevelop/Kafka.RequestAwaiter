using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using System.Reflection;
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
            PrivateFilds(sb, assemblyName, requestAwaiter);
            DisposeAsync(sb, requestAwaiter);
            Start(sb, requestAwaiter);

            StartTopicConsume(sb, assemblyName, requestAwaiter);
            StopConsume(sb, requestAwaiter);
            TryProduce(sb, requestAwaiter);
            TryProduceDelay(sb, requestAwaiter);
            Produce(sb, requestAwaiter);
            AddAwaiter(sb, assemblyName, requestAwaiter);
            RemoveAwaiter(sb);
            CreateOutputHeader(sb, assemblyName, requestAwaiter);

            End(sb);
        }

        private static void StartClassPartitionItem(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public class Bucket : IAsyncDisposable
            {{
                private readonly int _bucketId;
                public int BucketId => _bucketId;
                private readonly int _maxInFly;
                private ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
                private int _addedCount;
                private readonly Dictionary<string, {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse> _responseAwaiters;

                private CancellationTokenSource _ctsConsume;
                private Thread[] _consumeRoutines;
");
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
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                    string {inputData.NameCamelCase}Name,
                    int[] {inputData.NameCamelCase}Partitions
");
            }

            var consumerData = requestAwaiter.Data.ConsumerData;
            builder.Append($@",
                    int bucketId,
                    int maxInFly
");
            if(requestAwaiter.Data.UseLogger)
            {
                builder.Append($@",
                    ILogger logger
");
            }

            if (consumerData.CheckCurrentState)
            {
                builder.Append($@",
                    {consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState
");
            }

            if (consumerData.UseAfterCommit)
            {
                builder.Append($@",
                    {consumerData.AfterCommitFunc(requestAwaiter.InputDatas)} afterCommit
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                    string outputTopic{i}Name,
                    {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");
                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@",
                    {requestAwaiter.Data.AfterSendFunc(assemblyName, outputData, i)} afterSendOutput{i}
");
                }

                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                    {requestAwaiter.Data.LoadOutputMessageFunc(assemblyName, outputData, i, requestAwaiter.InputDatas)} loadOutput{i}Message
");
                }
            }
            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@",
                    {requestAwaiter.Data.AddAwaiterCheckStatusFunc(assemblyName, requestAwaiter.InputDatas)} addAwaiterCheckStatus
");
            }
            builder.Append($@"
                    )
                {{
                    _bucketId = bucketId;
                    _maxInFly = maxInFly;
                    _responseAwaiters = new(_maxInFly);

                    {(requestAwaiter.Data.UseLogger ? @"_logger = logger;" : "")}
                    {(consumerData.CheckCurrentState ? $"_getCurrentState = getCurrentState;" : "")}
                    {(consumerData.UseAfterCommit ? $"_afterCommit = afterCommit;" : "")}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                    _{inputData.NameCamelCase}Name = {inputData.NameCamelCase}Name;
                    _{inputData.NameCamelCase}Partitions = {inputData.NameCamelCase}Partitions;
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@"
                    _outputTopic{i}Name = outputTopic{i}Name;
                    _producerPool{i} = producerPool{i};
");
                if(requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@"
                    _afterSendOutput{i} = afterSendOutput{i};
");
                }

                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                    _loadOutput{i}Message = loadOutput{i}Message;
");
                }
            }

            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@"
                    _addAwaiterCheckStatus = addAwaiterCheckStatus;
");
            }

            builder.Append($@"
                }}
");
        }

        private static void PrivateFilds(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;
            if (requestAwaiter.Data.UseLogger)
            {
                builder.Append($@"
                private readonly ILogger _logger;
");
            }

            if (consumerData.CheckCurrentState)
            {
                builder.Append($@"
                private readonly {consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} _getCurrentState;
");
            }

            if (consumerData.UseAfterCommit)
            {
                builder.Append($@"
                private readonly {consumerData.AfterCommitFunc(requestAwaiter.InputDatas)} _afterCommit;
");
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                    private bool _consume{i}Canceled;
                    private TaskCompletionSource<List<Confluent.Kafka.TopicPartitionOffset>> _tcsPartitions{i};

                    private readonly string _{inputData.NameCamelCase}Name;
                    private readonly int[] _{inputData.NameCamelCase}Partitions;
                    public int[] {inputData.NamePascalCase}Partitions => _{inputData.NameCamelCase}Partitions;
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                    private readonly string _outputTopic{i}Name;
                    private readonly {outputData.FullPoolInterfaceName} _producerPool{i};
");
                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@"private readonly {requestAwaiter.Data.AfterSendFunc(assemblyName, outputData, i)} _afterSendOutput{i};");
                }

                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@"private readonly {requestAwaiter.Data.LoadOutputMessageFunc(assemblyName, outputData, i, requestAwaiter.InputDatas)} _loadOutput{i}Message;
");
                }
            }

            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@"private readonly {requestAwaiter.Data.AddAwaiterCheckStatusFunc(assemblyName, requestAwaiter.InputDatas)} _addAwaiterCheckStatus;
");
            }
        }

        private static void DisposeAsync(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async ValueTask DisposeAsync()
            {{
                await StopConsume();
                var isEmpty = false;
                while(!isEmpty)
                {{
                    await Task.Delay(50);
                    _lock.EnterReadLock();
                    try
                    {{
                        isEmpty |= _responseAwaiters.Count == 0;
                    }}
                    finally
                    {{ 
                        _lock.ExitReadLock();
                    }}
                }}

                _lock.Dispose();
                _lock = null;
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
                string groupId,
                Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                )
            {{
                _consumeRoutines = new Thread[{requestAwaiter.InputDatas.Count}];
                _ctsConsume = new CancellationTokenSource();
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                _consumeRoutines[{i}] = StartTopic{i}Consume(bootstrapServers, groupId, changeConfig);
                _consumeRoutines[{i}].Start();
                _consume{i}Canceled = false;
                _tcsPartitions{i} = new();
");
            }
            builder.Append($@"
            }}
");
        }

        private static void StartTopicConsume(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                private Thread StartTopic{i}Consume(
                    string bootstrapServers,
                    string groupId,
                    Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                    )
                {{
                    return new Thread((param) =>
                    {{
                        try
                        {{
                            var conf = new Confluent.Kafka.ConsumerConfig();
                            if(changeConfig != null)
                            {{
                                changeConfig(conf);
                            }}

                            conf.GroupId = $""{{groupId}}Bucket{{_bucketId}}"";
                            conf.BootstrapServers = bootstrapServers;
                            conf.AutoOffsetReset = AutoOffsetReset.Earliest;
                            conf.AllowAutoCreateTopics = false;
                            conf.EnableAutoCommit = false;

                            var consumer =
                                new ConsumerBuilder<{inputData.TypesPair}>(conf)
                                .Build()
                                ;

                            consumer.Assign(_{inputData.NameCamelCase}Partitions.Select(sel => new TopicPartition(_{inputData.NameCamelCase}Name, sel)));
                            try
                            {{
                                var offsets = new Dictionary<Partition, TopicPartitionOffset>();
                                while (!_ctsConsume.Token.IsCancellationRequested)
                                {{
                                    try
                                    {{
                                        ConsumeResult<{inputData.TypesPair}> consumeResult = consumer.Consume(50);
                                        try
                                        {{
                                            _ctsConsume.Token.ThrowIfCancellationRequested();
                                        }}
                                        catch (OperationCanceledException oce)
                                        {{
                                            Volatile.Read(ref _tcsPartitions{i})?.TrySetCanceled();
                                            _lock.EnterReadLock();
                                            try
                                            {{
                                                foreach (var topicResponseItem in _responseAwaiters.Values)
                                                {{
");
                if(inputData.AcceptFromAny)
                {
                    builder.Append($@"
                                                    topicResponseItem.TrySetException({i}, oce);
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                                                    topicResponseItem.TrySetException({i}, oce, {j});
");
                    }
                }
                builder.Append($@"
                                                }}
                                            }}
                                            finally
                                            {{
                                                _lock.ExitReadLock();
                                            }}
                                            throw;
                                        }}
                                        {requestAwaiter.Data.TypeSymbol.Name}.{inputData.MessageTypeName} inputMessage = null;
                                        if(consumeResult != null)
                                        {{
                                            if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                            {{
                                                {LogInputMessage(requestAwaiter, inputData, "LogError")}
                                                offsets[consumeResult.Partition] = consumeResult.TopicPartitionOffset;
                                                continue;
                                            }}

                                            var headerInfo = {assemblyName}.ResponseHeader.Parser.ParseFrom(infoBytes);
                                            if(headerInfo.Bucket != _bucketId)
                                            {{
                                                offsets[consumeResult.Partition] = consumeResult.TopicPartitionOffset;
                                                continue;
                                            }}

                                            inputMessage = new()
                                            {{
                                                OriginalMessage = consumeResult.Message,
                                                {(inputData.KeyType.IsKafkaNull() ? "" : $"Key = {GetResponseKey(inputData)},")}
                                                Value = {GetResponseValue(inputData)},
                                                Partition = consumeResult.Partition,
                                                HeaderInfo = headerInfo,
                                                TopicName = _{inputData.NameCamelCase}Name
                                            }};

                                            {LogInputMessage(requestAwaiter, inputData, "LogInformation")}
                                        }}
                                        while (true) 
                                        {{
                                            var locked = _lock.TryEnterUpgradeableReadLock(50);
                                            if(locked)
                                            {{
                                                {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse topicResponse = null;
                                                try
                                                {{
                                                    if (inputMessage != null)
                                                    {{
                                                        _responseAwaiters.TryGetValue(inputMessage.HeaderInfo.AnswerToMessageGuid, out topicResponse);
                                                    }}

                                                    if (_addedCount == _maxInFly && _responseAwaiters.Count == 0)
                                                    {{
                                                        _lock.EnterWriteLock();
                                                        try
                                                        {{
                                                            var allPartitions = offsets.Values.ToList();
");
                for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                {
                    if(i == j)
                    {
                        continue;
                    }

                    builder.Append($@"
                                                            var tcsPartitions{j} = new TaskCompletionSource<List<Confluent.Kafka.TopicPartitionOffset>>();
                                                            Volatile.Write(ref _tcsPartitions{j}, tcsPartitions{j});
                                                            Volatile.Write(ref _consume{j}Canceled, true);
                                                            var partitions{j} = tcsPartitions{j}.Task.Result;
                                                            allPartitions.AddRange(partitions{j});
");
                }
                builder.Append($@"
                                                            if(allPartitions.Count != 0)
                                                                consumer.Commit(allPartitions);
                                                            _addedCount = 0;
");
                if(consumerData.UseAfterCommit)
                {
                    builder.Append($@"
                                                            _afterCommit(
                                                                _bucketId,
                                                                offsets.Keys.ToHashSet()
");
                    for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                    {
                        if (i == j)
                        {
                            continue;
                        }

                        builder.Append($@",
                                                                partitions{j}.Select(sel => sel.Partition).ToHashSet()
");
                    }
                    builder.Append($@"
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

                                                if(topicResponse != null)
                                                {{
");
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                                                    topicResponse.TrySetResponse({i}, inputMessage);
");
                }
                else
                {
                    builder.Append($@"
                                                    switch(inputMessage.HeaderInfo.AnswerFrom)
                                                    {{
                                                        default:
                                                        {{
                                                            //ignore
                                                            break;
                                                        }}
");

                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                                                        case ""{inputData.AcceptedService[j]}"":
                                                        {{
                                                            topicResponse.TrySetResponse({i}, inputMessage, {j});
                                                            break;
                                                        }}
");
                    }
                    builder.Append($@"
                                                    }}
");
                }
                builder.Append($@"
                                                }}
                                            
                                                break;
                                            }}
                                            else if(Volatile.Read(ref _consume{i}Canceled))
                                            {{
                                                Volatile.Write(ref _consume{i}Canceled, false);
                                                Volatile.Read(ref _tcsPartitions{i}).SetResult(offsets.Values.ToList());
                                            }}
                                        }}

                                        if(consumeResult != null)
                                        {{
                                            offsets[consumeResult.Partition] = consumeResult.TopicPartitionOffset;
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
                        }}
                        catch(OperationCanceledException)
                        {{
                            //ignore
                        }}
                        catch(AggregateException agg)
                        {{
                            if(!agg.InnerExceptions.All(an => an is OperationCanceledException))
                            {{
                                throw;
                            }}
                        }}
                    }}
                )
                    {{
                        IsBackground = true,
                        Priority = ThreadPriority.AboveNormal,
                        Name = $""{{groupId}}Bucket{{_bucketId}}Topic{i}""
                    }};
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
                private async ValueTask StopConsume()
                {{
                    _ctsConsume?.Cancel();
");
            builder.Append($@"
                    _lock.EnterWriteLock();
                    try
                    {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                        Volatile.Write(ref _consume{i}Canceled, true);
");
            }
            builder.Append($@"
                    }}
                    finally
                    {{ 
                        _lock.ExitWriteLock();
                    }}
                    
                    foreach (var consumeRoutine in _consumeRoutines)
                    {{
                        while(consumeRoutine.IsAlive)
                        {{
                            await Task.Delay(50);
                        }}
                    }}

                    _ctsConsume?.Dispose();
                }}
");
        }

        private static void TryProduce(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;

            builder.Append($@"
            public async ValueTask<{requestAwaiter.TypeSymbol.Name}.TryProduceResult> TryProduce(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                {requestAwaiter.OutputDatas[i].KeyType.GetFullTypeName(true)} key{i},
");
                }

                builder.Append($@"
                {requestAwaiter.OutputDatas[i].ValueType.GetFullTypeName(true)} value{i},
");
            }
            builder.Append($@"
                int waitResponseTimeout = 0
                )
            {{
                string messageGuid = null;
                {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse awaiter = null;

                var needDispose = false;
                _lock.EnterUpgradeableReadLock();
                try
                {{
                    if(_addedCount == _maxInFly)
                    {{
                        return new {requestAwaiter.TypeSymbol.Name}.TryProduceResult {{ Succsess = false }};
                    }}
                    else
                    {{
                        messageGuid = Guid.NewGuid().ToString(""D"");
                        awaiter = 
                            new {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse(
                                _bucketId,
                                {(consumerData.CheckCurrentState ? $"_getCurrentState," : "")}
                                messageGuid,
                                RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                _{inputData.NameCamelCase}Partitions
");
            }
            builder.Append($@",
                                waitResponseTimeout
                                );

                        _lock.EnterWriteLock();
                        try
                        {{
                            if (!_responseAwaiters.TryAdd(messageGuid, awaiter))
                            {{
                                needDispose = true;
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

                if(needDispose)
                {{
                    awaiter.Dispose(); 
                    return new {requestAwaiter.TypeSymbol.Name}.TryProduceResult {{Succsess = false}};
                }}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                CreateOutputMessage(builder, requestAwaiter, outputData, i);
                builder.Append($@"
                var header{i} = CreateOutputHeader();
");

                builder.Append($@"
                header{i}.MessageGuid = messageGuid;
                message{i}.Headers = new Headers
                {{
                    {{ ""Info"", header{i}.ToByteArray() }}
                }};
");
            }
            
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                var variable = i == 0 ? "var producer" : "producer";
                builder.Append($@"
                {variable} = _producerPool{i}.Rent();
                try
                {{
                    var deliveryResult = await producer.ProduceAsync(_outputTopic{i}Name, message{i}).ConfigureAwait(false);
                }}
                catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.Data.UseLogger ? "e" : "")})
                {{
                        {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
                        _lock.EnterWriteLock();
                        try
                        {{
                            _responseAwaiters.Remove(header{i}.MessageGuid, out _);
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
                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@"
                await _afterSendOutput{i}(
                        header{i},
                        new Output{i}Message(
                                message{i}
");
                    if (outputData.KeyType.IsProtobuffType())
                    {
                        builder.Append($@",
                                key{i}
");
                    }

                    if (outputData.ValueType.IsProtobuffType())
                    {
                        builder.Append($@",
                                value{i}
");
                    }
                    builder.Append($@"
                                )
                        )
                        .ConfigureAwait(false)
                        ;
");
                }
            }
            builder.Append($@"
                var response = await awaiter.GetResponse().ConfigureAwait(false);
                return new {requestAwaiter.TypeSymbol.Name}.TryProduceResult() 
                {{
                    Succsess = true,
                    Response = response
                }};
            }}
");
        }

        private static void TryProduceDelay(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;

            builder.Append($@"
            public {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult TryProduceDelay(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                {requestAwaiter.OutputDatas[i].KeyType.GetFullTypeName(true)} key{i},
");
                }

                builder.Append($@"
                {requestAwaiter.OutputDatas[i].ValueType.GetFullTypeName(true)} value{i},
");
            }
            builder.Append($@"
                int waitResponseTimeout = 0
                )
            {{
                string messageGuid = null;
                {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse awaiter = null;

                var needDispose = false;
                _lock.EnterUpgradeableReadLock();
                try
                {{
                    if(_addedCount == _maxInFly)
                    {{
                        return new {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult {{ Succsess = false }};
                    }}
                    else
                    {{
                        messageGuid = Guid.NewGuid().ToString(""D"");
                        awaiter = 
                            new {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse(
                                    _bucketId,
                                    {(consumerData.CheckCurrentState ? $"_getCurrentState," : "")}
                                    messageGuid,
                                    RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                _{inputData.NameCamelCase}Partitions
");
            }
            builder.Append($@",
                                    waitResponseTimeout
                                    );

                        _lock.EnterWriteLock();
                        try
                        {{
                            if (!_responseAwaiters.TryAdd(messageGuid, awaiter))
                            {{
                                needDispose = true;
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

                if(needDispose)
                {{
                    awaiter.Dispose(); 
                    return new {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult {{Succsess = false}};
                }}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                CreateOutputMessage(builder, requestAwaiter, outputData, i);
                builder.Append($@"
                var header{i} = CreateOutputHeader();
");

                builder.Append($@"
                header{i}.MessageGuid = messageGuid;
                message{i}.Headers = new Headers
                {{
                    {{ ""Info"", header{i}.ToByteArray() }}
                }};
");
            }
            builder.Append($@"
                return new {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult 
                {{
                    Succsess = true, 
                    Response = awaiter, 
                    Bucket = this
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                    {outputData.NamePascalCase}Header = header{i},
                    {outputData.MessageTypeName} = 
                        new {outputData.MessageTypeName}(
                                message{i}
");
                if (outputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@",
                                key{i}
");
                }

                if (outputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@",
                                value{i}
");
                }
                builder.Append($@"
                                )
");
            }
            builder.Append($@"
                }};
            }}
");
        }

        private static void Produce(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;

            builder.Append($@"
            public async ValueTask<{requestAwaiter.TypeSymbol.Name}.Response> Produce(
                {requestAwaiter.Data.TypeSymbol.Name}.TryDelayProduceResult tryDelayProduce
                )
            {{
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                var variable = i == 0 ? "var producer" : "producer";
                builder.Append($@"
                {variable} = _producerPool{i}.Rent();
                try
                {{
                    var deliveryResult = await producer.ProduceAsync(_outputTopic{i}Name, tryDelayProduce.{outputData.MessageTypeName}.Message).ConfigureAwait(false);
                }}
                catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.Data.UseLogger ? "e" : "")})
                {{
                        {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
                        _lock.EnterWriteLock();
                        try
                        {{
                            _responseAwaiters.Remove(tryDelayProduce.Response.MessageGuid, out _);
                        }}
                        finally
                        {{
                            _lock.ExitWriteLock();
                        }}
                    tryDelayProduce.Response.Dispose();

                    throw;
                }}
                finally
                {{
                    _producerPool{i}.Return(producer);
                }}
");
                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@"
                await _afterSendOutput{i}(
                        tryDelayProduce.{outputData.NamePascalCase}Header,
                        tryDelayProduce.{outputData.MessageTypeName})
                        .ConfigureAwait(false)
                        ;");
                }
            }
            builder.Append($@"
                return 
                    await tryDelayProduce.Response.GetResponse().ConfigureAwait(false);
            }}
");
        }

        private static void CreateOutputMessage(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter,
            OutputData outputData,
            int messageNum
            )
        {
            builder.Append($@"
                var message{messageNum} = new Confluent.Kafka.Message<{outputData.TypesPair}>()
                {{
");

            if (!outputData.KeyType.IsKafkaNull())
            {
                builder.Append($@"
                    Key = {(outputData.KeyType.IsProtobuffType() ? $"key{messageNum}.ToByteArray()" : $"key{messageNum}")},
");
            }

            builder.Append($@"
                    Value = {(outputData.ValueType.IsProtobuffType() ? $"value{messageNum}.ToByteArray()" : $"value{messageNum}")}
                }};
");
        }

        private static void AddAwaiter(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;
            var returnType = 
                requestAwaiter.Data.AddAwaiterCheckStatus ? 
                    $"async ValueTask<{requestAwaiter.Data.TypeSymbol.Name}.TopicResponse>" : 
                    $"{requestAwaiter.Data.TypeSymbol.Name}.TopicResponse"
                    ;
            builder.Append($@"
            public {returnType} AddAwaiter(
                string messageGuid,
                int waitResponseTimeout = 0
                )
            {{
                {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse awaiter = null;
                
                var needDispose = false;
                _lock.EnterUpgradeableReadLock();
                try
                {{
                    if (_addedCount == _maxInFly)
                    {{
                        throw new InvalidOperationException(""Expect awaiter's limit exceeded"");
                    }}
                    else
                    {{
                        awaiter =
                            new {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse(
                                    _bucketId,
                                    {(consumerData.CheckCurrentState ? $"_getCurrentState," : "")}
                                    messageGuid,
                                    RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                _{inputData.NameCamelCase}Partitions
");
            }
            builder.Append($@",
                                    waitResponseTimeout
                                    );

                        _lock.EnterWriteLock();
                        try
                        {{
                            if (!_responseAwaiters.TryAdd(messageGuid, awaiter))
                            {{
                                needDispose = true;
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

                if (needDispose)
                {{
                    awaiter.Dispose();
                    throw new InvalidOperationException(""Duplicate awaiter key"");
                }}
");
            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
                {
                    var outputData = requestAwaiter.OutputDatas[i];
                    builder.Append($@"
                {{
                    var status = 
                        await _addAwaiterCheckStatus(
                                messageGuid,
                                _bucketId
");
                    for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                    {
                        var inputData = requestAwaiter.InputDatas[j];
                        builder.Append($@",
                                _{inputData.NameCamelCase}Partitions
");
                    }
                    builder.Append($@"
                                )
                                .ConfigureAwait(false);

                    if(status < KafkaExchanger.Attributes.Enums.RAState.Sended)
                    {{
                        var message = 
                            await _loadOutput{i}Message(
                                    messageGuid,
                                    _bucketId
");
                    for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                    {
                        var inputData = requestAwaiter.InputDatas[j];
                        builder.Append($@",
                                    _{inputData.NameCamelCase}Partitions
");
                    }
                    builder.Append($@"
                                    )
                                    .ConfigureAwait(false);
");
                    builder.Append($@"
                        var header = CreateOutputHeader();
                        header.MessageGuid = messageGuid;
                        message.Message.Headers = new Headers
                        {{
                            {{ ""Info"", header.ToByteArray() }}
                        }};

                        var producer = _producerPool{i}.Rent();
                        try
                        {{
                            var deliveryResult = await producer.ProduceAsync(_outputTopic{i}Name, message.Message).ConfigureAwait(false);
                        }}
                        catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.Data.UseLogger ? "e" : "")})
                        {{
                            {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
                            _lock.EnterWriteLock();
                            try
                            {{
                                _responseAwaiters.Remove(awaiter.MessageGuid, out _);
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
                    if (requestAwaiter.Data.AfterSend)
                    {
                        builder.Append($@"
                        await _afterSendOutput{i}(header, message).ConfigureAwait(false);
");
                    }
                    builder.Append($@"
                    }}
                }}
");
                }
            }
            builder.Append($@"

                return awaiter;
            }}
");

        }

        private static void RemoveAwaiter(StringBuilder builder)
        {
            builder.Append($@"
            public void RemoveAwaiter(string guid)
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

        private static void CreateOutputHeader(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private {assemblyName}.RequestHeader CreateOutputHeader()
            {{
                var headerInfo = new {assemblyName}.RequestHeader()
                {{
                    Bucket = _bucketId
                }};
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                var variable = i == 0 ? $"var topic" : $"topic";
                builder.Append($@"
                {variable} = new {assemblyName}.Topic()
                {{
                    Name = _{inputData.NameCamelCase}Name
                }};
                topic.Partitions.Add(_{inputData.NameCamelCase}Partitions);
");
                if (!inputData.AcceptFromAny)
                {
                    builder.Append($@"
                topic.CanAnswerFrom.Add(new string[]{{
");
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
                headerInfo.TopicsForAnswer.Add(topic);
");

            }
            builder.Append($@"
                return headerInfo;
            }}
");
        }

        private static string LogInputMessage(
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter,
            InputData inputData,
            string logMethod,
            string afterMessageInfo = ""
            )
        {
            if (!requestAwaiter.Data.UseLogger)
            {
                return string.Empty;
            }

            var temp = new StringBuilder(170);
            temp.Append($@"_logger.{logMethod}($""Consumed inputMessage Key: ");
            if (inputData.KeyType.IsProtobuffType())
            {
                temp.Append(@"'{inputMessage.Key}'");
            }
            else
            {
                temp.Append(@"'{consumeResult.Message.Key}'");
            }
            temp.Append($@", Value: ");
            if (inputData.ValueType.IsProtobuffType())
            {
                temp.Append(@"'{inputMessage.Value}'");
            }
            else
            {
                temp.Append(@"'{consumeResult.Message.Value}'");
            }
            temp.Append($@"{afterMessageInfo}."");");

            return temp.ToString();
        }

        private static string GetResponseKey(InputData inputData)
        {
            if (inputData.KeyType.IsProtobuffType())
            {
                return $"{inputData.KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private static string GetResponseValue(InputData inputData)
        {
            if (inputData.ValueType.IsProtobuffType())
            {
                return $"{inputData.ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
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