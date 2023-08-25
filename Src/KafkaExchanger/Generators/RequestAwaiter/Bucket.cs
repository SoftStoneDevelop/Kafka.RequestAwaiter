using KafkaExchanger.Datas;
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public class Bucket : IAsyncDisposable
            {{
                private readonly int {BucketNames.PBucketId()};
                public int {BucketNames.BucketId()} => {BucketNames.PBucketId()};
                private readonly int {BucketNames.PMaxInFly()};
                private ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
                private int _addedCount;
                private readonly Dictionary<string, {requestAwaiter.TypeSymbol.Name}.TopicResponse> _responseAwaiters;

                private CancellationTokenSource _ctsConsume;
                private Thread[] _consumeRoutines;
");
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
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

            builder.Append($@",
                    int {BucketNames.BucketId().ToCamel()},
                    int {ProcessorConfig.MaxInFlyNameCamel()}
");
            if(requestAwaiter.UseLogger)
            {
                builder.Append($@",
                    ILogger logger
");
            }

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@",
                    {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {ProcessorConfig.CurrentStateFuncNameCamel()}
");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@",
                    {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {ProcessorConfig.AfterCommitFuncNameCamel()}
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                    string {BucketNames.OutputTopicName(outputData).ToCamel()},
                    {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",
                    {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {ProcessorConfig.AfterSendFuncNameCamel(outputData)}
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                    {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {ProcessorConfig.LoadOutputFuncNameCamel(outputData)},
                    {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {ProcessorConfig.CheckOutputStatusFuncNameCamel(outputData)}
");
                }
            }
            builder.Append($@"
                    )
                {{
                    {BucketNames.PBucketId()} = {BucketNames.BucketId().ToCamel()};
                    {BucketNames.PMaxInFly()} = {ProcessorConfig.MaxInFlyNameCamel()};
                    _responseAwaiters = new({BucketNames.PMaxInFly()});

                    {(requestAwaiter.UseLogger ? @"_logger = logger;" : "")}
                    {(requestAwaiter.CheckCurrentState ? $"{BucketNames.PCurrentStateFunc()} = {ProcessorConfig.CurrentStateFuncNameCamel()};" : "")}
                    {(requestAwaiter.AfterCommit ? $"{BucketNames.PAfterCommitFunc()} = {ProcessorConfig.AfterCommitFuncNameCamel()};" : "")}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                    _{inputData.NameCamelCase}Name = {inputData.NameCamelCase}Name;
                    {BucketNames.Partitions(inputData)} = {inputData.NameCamelCase}Partitions;
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                    {BucketNames.POutputTopicName(outputData)} = {BucketNames.OutputTopicName(outputData).ToCamel()};
                    _producerPool{i} = producerPool{i};
");
                if(requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                    {BucketNames.PAfterSendFunc(outputData)} = {ProcessorConfig.AfterSendFuncNameCamel(outputData)};
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                    {BucketNames.PLoadOutputFunc(outputData)} = {ProcessorConfig.LoadOutputFuncNameCamel(outputData)};
                    {BucketNames.PCheckOutputStatusFunc(outputData)} = {ProcessorConfig.CheckOutputStatusFuncNameCamel(outputData)};
");
                }
            }

            builder.Append($@"
                }}
");
        }

        private static void PrivateFilds(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            if (requestAwaiter.UseLogger)
            {
                builder.Append($@"
                private readonly ILogger _logger;
");
            }

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                private readonly {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {BucketNames.PCurrentStateFunc()};
");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                private readonly {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {BucketNames.PAfterCommitFunc()};
");
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                    private bool {BucketNames.PConsumeCanceledName(inputData)};
                    private TaskCompletionSource<List<Confluent.Kafka.TopicPartitionOffset>> {BucketNames.PTCSPartitionsName(inputData)};

                    private readonly string _{inputData.NameCamelCase}Name;
                    public int[] {BucketNames.Partitions(inputData)} {{ get; init; }}
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                    private readonly string {BucketNames.POutputTopicName(outputData)};
                    private readonly {outputData.FullPoolInterfaceName} _producerPool{i};
");
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"private readonly {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {BucketNames.PAfterSendFunc(outputData)};");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                    private readonly {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {BucketNames.PLoadOutputFunc(outputData)};
                    private readonly {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {BucketNames.PCheckOutputStatusFunc(outputData)};
");
                }
            }
        }

        private static void DisposeAsync(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
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
                _consumeRoutines = new Thread[{requestAwaiter.InputDatas.Count}];
                _ctsConsume = new CancellationTokenSource();
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                _consumeRoutines[{i}] = StartTopic{i}Consume(bootstrapServers, groupId, changeConfig);
                _consumeRoutines[{i}].Start();
                {BucketNames.PConsumeCanceledName(inputData)} = false;
                {BucketNames.PTCSPartitionsName(inputData)} = new();
");
            }
            builder.Append($@"
            }}
");
        }

        private static void StartTopicConsume(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
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

                            conf.GroupId = $""{{groupId}}Bucket{{{BucketNames.PBucketId()}}}"";
                            conf.BootstrapServers = bootstrapServers;
                            conf.AutoOffsetReset = AutoOffsetReset.Earliest;
                            conf.AllowAutoCreateTopics = false;
                            conf.EnableAutoCommit = false;

                            var consumer =
                                new ConsumerBuilder<{inputData.TypesPair}>(conf)
                                .Build()
                                ;

                            consumer.Assign({BucketNames.Partitions(inputData)}.Select(sel => new TopicPartition(_{inputData.NameCamelCase}Name, sel)));
                            try
                            {{
                                var offsets = new Dictionary<Partition, TopicPartitionOffset>();
                                while (!_ctsConsume.Token.IsCancellationRequested)
                                {{
                                    try
                                    {{
                                        ConsumeResult<{inputData.TypesPair}> consumeResult = consumer.Consume(30);
                                        try
                                        {{
                                            _ctsConsume.Token.ThrowIfCancellationRequested();
                                        }}
                                        catch (OperationCanceledException oce)
                                        {{
                                            Volatile.Read(ref {BucketNames.PTCSPartitionsName(inputData)})?.TrySetCanceled();
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
                                        {requestAwaiter.TypeSymbol.Name}.{inputData.MessageTypeName} inputMessage = null;
                                        if(consumeResult != null)
                                        {{
                                            if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                            {{
                                                {LogInputMessage(requestAwaiter, inputData, "LogError")}
                                                offsets[consumeResult.Partition] = consumeResult.TopicPartitionOffset;
                                                continue;
                                            }}

                                            var headerInfo = {assemblyName}.ResponseHeader.Parser.ParseFrom(infoBytes);
                                            if(headerInfo.Bucket != {BucketNames.PBucketId()})
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
                                            var locked = _lock.TryEnterUpgradeableReadLock(15);
                                            if(locked)
                                            {{
                                                {requestAwaiter.TypeSymbol.Name}.TopicResponse topicResponse = null;
                                                try
                                                {{
                                                    if (inputMessage != null)
                                                    {{
                                                        _responseAwaiters.TryGetValue(inputMessage.HeaderInfo.AnswerToMessageGuid, out topicResponse);
                                                    }}

                                                    if (_addedCount == {BucketNames.PMaxInFly()} && _responseAwaiters.Count == 0)
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

                    var inputDataNotSelf = requestAwaiter.InputDatas[j];
                    builder.Append($@"
                                                            var tcsPartitions{j} = new TaskCompletionSource<List<Confluent.Kafka.TopicPartitionOffset>>();
                                                            Volatile.Write(ref {BucketNames.PTCSPartitionsName(inputDataNotSelf)}, tcsPartitions{j});
                                                            Volatile.Write(ref {BucketNames.PConsumeCanceledName(inputDataNotSelf)}, true);
                                                            var partitions{j} = tcsPartitions{j}.Task.Result;
                                                            allPartitions.AddRange(partitions{j});
");
                }
                builder.Append($@"
                                                            if(allPartitions.Count != 0)
                                                                consumer.Commit(allPartitions);
                                                            _addedCount = 0;
");
                if(requestAwaiter.AfterCommit)
                {
                    builder.Append($@"
                                                            {BucketNames.PAfterCommitFunc()}(
                                                                {BucketNames.PBucketId()},
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
                                            else if(Volatile.Read(ref {BucketNames.PConsumeCanceledName(inputData)}))
                                            {{
                                                Volatile.Write(ref {BucketNames.PConsumeCanceledName(inputData)}, false);
                                                Volatile.Read(ref {BucketNames.PTCSPartitionsName(inputData)}).SetResult(offsets.Values.ToList());
                                            }}
                                        }}

                                        if(consumeResult != null)
                                        {{
                                            offsets[consumeResult.Partition] = consumeResult.TopicPartitionOffset;
                                        }}
                                    }}
                                    catch (ConsumeException {(requestAwaiter.UseLogger ? "e" : "")})
                                    {{
                                        {(requestAwaiter.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "//ignore")}
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
                        Name = $""{{groupId}}Bucket{{{BucketNames.PBucketId()}}}Topic{i}""
                    }};
                }}
");
            }
        }

        private static void StopConsume(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
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
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                        Volatile.Write(ref {BucketNames.PConsumeCanceledName(inputData)}, true);
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
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
                {requestAwaiter.TypeSymbol.Name}.TopicResponse awaiter = null;

                var needDispose = false;
                _lock.EnterUpgradeableReadLock();
                try
                {{
                    if(_addedCount == {BucketNames.PMaxInFly()})
                    {{
                        return new {requestAwaiter.TypeSymbol.Name}.TryProduceResult {{ Succsess = false }};
                    }}
                    else
                    {{
                        messageGuid = Guid.NewGuid().ToString(""D"");
                        awaiter = 
                            new {requestAwaiter.TypeSymbol.Name}.TopicResponse(
                                {BucketNames.PBucketId()},
                                {(requestAwaiter.CheckCurrentState ? $"{BucketNames.PCurrentStateFunc()}," : "")}
                                messageGuid,
                                RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                {BucketNames.Partitions(inputData)}
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
                    var deliveryResult = await producer.ProduceAsync({BucketNames.POutputTopicName(outputData)}, message{i}).ConfigureAwait(false);
                }}
                catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.UseLogger ? "e" : "")})
                {{
                        {(requestAwaiter.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
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
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                await {BucketNames.PAfterSendFunc(outputData)}(
                        header{i},
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult TryProduceDelay(
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
                {requestAwaiter.TypeSymbol.Name}.TopicResponse awaiter = null;

                var needDispose = false;
                _lock.EnterUpgradeableReadLock();
                try
                {{
                    if(_addedCount == {BucketNames.PMaxInFly()})
                    {{
                        return new {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult {{ Succsess = false }};
                    }}
                    else
                    {{
                        messageGuid = Guid.NewGuid().ToString(""D"");
                        awaiter = 
                            new {requestAwaiter.TypeSymbol.Name}.TopicResponse(
                                    {BucketNames.PBucketId()},
                                    {(requestAwaiter.CheckCurrentState ? $"{BucketNames.PCurrentStateFunc()}," : "")}
                                    messageGuid,
                                    RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                {BucketNames.Partitions(inputData)}
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
                    return new {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult {{Succsess = false}};
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
                return new {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult 
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async ValueTask<{requestAwaiter.TypeSymbol.Name}.Response> Produce(
                {requestAwaiter.TypeSymbol.Name}.TryDelayProduceResult tryDelayProduce
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
                    var deliveryResult = await producer.ProduceAsync({BucketNames.POutputTopicName(outputData)}, tryDelayProduce.{outputData.MessageTypeName}.Message).ConfigureAwait(false);
                }}
                catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.UseLogger ? "e" : "")})
                {{
                        {(requestAwaiter.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
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
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                await {BucketNames.PAfterSendFunc(outputData)}(
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter,
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            var returnType = 
                requestAwaiter.AddAwaiterCheckStatus ? 
                    $"async ValueTask<{requestAwaiter.TypeSymbol.Name}.TopicResponse>" : 
                    $"{requestAwaiter.TypeSymbol.Name}.TopicResponse"
                    ;
            builder.Append($@"
            public {returnType} AddAwaiter(
                string messageGuid,
                int waitResponseTimeout = 0
                )
            {{
                {requestAwaiter.TypeSymbol.Name}.TopicResponse awaiter = null;
                
                var needDispose = false;
                _lock.EnterUpgradeableReadLock();
                try
                {{
                    if (_addedCount == {BucketNames.PMaxInFly()})
                    {{
                        throw new InvalidOperationException(""Expect awaiter's limit exceeded"");
                    }}
                    else
                    {{
                        awaiter =
                            new {requestAwaiter.TypeSymbol.Name}.TopicResponse(
                                    {BucketNames.PBucketId()},
                                    {(requestAwaiter.CheckCurrentState ? $"{BucketNames.PCurrentStateFunc()}," : "")}
                                    messageGuid,
                                    RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                {BucketNames.Partitions(inputData)}
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
            if (requestAwaiter.AddAwaiterCheckStatus)
            {
                for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
                {
                    var outputData = requestAwaiter.OutputDatas[i];
                    builder.Append($@"
                {{
                    var status = 
                        await {BucketNames.PCheckOutputStatusFunc(outputData)}(
                                messageGuid,
                                {BucketNames.PBucketId()}
");
                    for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                    {
                        var inputData = requestAwaiter.InputDatas[j];
                        builder.Append($@",
                                {BucketNames.Partitions(inputData)}
");
                    }
                    builder.Append($@"
                                )
                                .ConfigureAwait(false);

                    if(status < KafkaExchanger.Attributes.Enums.RAState.Sended)
                    {{
                        var message = 
                            await {BucketNames.PLoadOutputFunc(outputData)}(
                                    messageGuid,
                                    {BucketNames.PBucketId()}
");
                    for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                    {
                        var inputData = requestAwaiter.InputDatas[j];
                        builder.Append($@",
                                    {BucketNames.Partitions(inputData)}
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
                            var deliveryResult = await producer.ProduceAsync({BucketNames.POutputTopicName(outputData)}, message.Message).ConfigureAwait(false);
                        }}
                        catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.UseLogger ? "e" : "")})
                        {{
                            {(requestAwaiter.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
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
                    if (requestAwaiter.AfterSend)
                    {
                        builder.Append($@"
                        await {BucketNames.PAfterSendFunc(outputData)}(header, message).ConfigureAwait(false);
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private {assemblyName}.RequestHeader CreateOutputHeader()
            {{
                var headerInfo = new {assemblyName}.RequestHeader()
                {{
                    Bucket = {BucketNames.PBucketId()}
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
                topic.Partitions.Add({BucketNames.Partitions(inputData)});
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter,
            InputData inputData,
            string logMethod,
            string afterMessageInfo = ""
            )
        {
            if (!requestAwaiter.UseLogger)
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