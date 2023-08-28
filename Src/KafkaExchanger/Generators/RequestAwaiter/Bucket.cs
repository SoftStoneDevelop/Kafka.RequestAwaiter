using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
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
            StopAsync(sb, requestAwaiter);
            Start(sb, requestAwaiter);

            StartTopicConsume(sb, assemblyName, requestAwaiter);
            StopConsume(sb, requestAwaiter);
            TryProduceDelay(sb, requestAwaiter);
            Produce(sb, requestAwaiter);
            AddAwaiter(sb, assemblyName, requestAwaiter);
            RemoveAwaiter(sb);
            CreateOutputHeader(sb, assemblyName, requestAwaiter);

            End(sb);
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{PartitionItem.TypeName()}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "Bucket";
        }

        public static string BucketId()
        {
            return "BucketId";
        }

        private static string _bucketId()
        {
            return "_bucketId";
        }

        private static string _outputTopicName(OutputData outputData)
        {
            return $"_{outputData.NameCamelCase}TopicName";
        }

        public static string _maxInFly()
        {
            return "_maxInFly";
        }

        private static string _tcsPartitions(InputData inputData)
        {
            return $"_tcsPartitions{inputData.NamePascalCase}";
        }

        private static string _consumeCanceled(InputData inputData)
        {
            return $"_consume{inputData.NamePascalCase}Canceled";
        }

        private static string _currentStateFunc()
        {
            return "_currentState";
        }

        private static string _afterCommitFunc()
        {
            return "_afterCommit";
        }

        private static string _afterSendFunc(OutputData outputData)
        {
            return $"_afterSend{outputData.NamePascalCase}";
        }

        private static string _loadOutputFunc(OutputData outputData)
        {
            return $"_load{outputData.MessageTypeName}";
        }

        private static string loadOutputFuncName(OutputData outputData)
        {
            return $"load{outputData.MessageTypeName}";
        }

        private static string _checkOutputStatusFunc(OutputData outputData)
        {
            return $"_check{outputData.NamePascalCase}Status";
        }

        public static string Partitions(InputData inputData)
        {
            return $"{inputData.NamePascalCase}Partitions";
        }

        public static string _inputTopicName(InputData inputData)
        {
            return $"_{inputData.NameCamelCase}Name";
        }

        private static string _fws()
        {
            return $"_fws";
        }

        private static string _lock()
        {
            return $"_lock";
        }

        private static string _addedCount()
        {
            return $"_addedCount";
        }

        private static string _responseAwaiters()
        {
            return $"_responseAwaiters";
        }

        private static string _consumeRoutines()
        {
            return $"_consumeRoutines";
        }

        private static string _ctsConsume()
        {
            return $"_ctsConsume";
        }

        private static string _producerPool(OutputData outputData)
        {
            return $"_{outputData.NameCamelCase}Pool";
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
                private readonly KafkaExchanger.FreeWatcherSignal {_fws()};
                private readonly int {_bucketId()};
                public int {BucketId()} => {_bucketId()};
                private readonly int {_maxInFly()};
                private ReaderWriterLockSlim {_lock()} = new ReaderWriterLockSlim();
                private int {_addedCount()};
                private readonly Dictionary<string, {TopicResponse.TypeFullName(requestAwaiter)}> {_responseAwaiters()};

                private CancellationTokenSource {_ctsConsume()};
                private Thread[] {_consumeRoutines()};
");
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

            string outputTopicName(OutputData outputData)
            {
                return $"{outputData.NameCamelCase}TopicName";
            }

            string afterSendFunc(OutputData outputData)
            {
                return $"afterSend{outputData.NamePascalCase}";
            }

            builder.Append($@"
                public {TypeName()}(
                    KafkaExchanger.FreeWatcherSignal fws,
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
                    int bucketId,
                    int maxInFly
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
                    {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} currentState
");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@",
                    {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} afterCommit
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                    string {outputTopicName(outputData)},
                    {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",
                    {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {afterSendFunc(outputData)}
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                    {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {loadOutputFuncName(outputData)},
                    {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {checkOutputStatusFunc(outputData)}
");
                }
            }
            builder.Append($@"
                    )
                {{
                    {_fws()} = fws;
                    {_bucketId()} = bucketId;
                    {_maxInFly()} = maxInFly;
                    {_responseAwaiters()} = new({_maxInFly()});

                    {(requestAwaiter.UseLogger ? @"_logger = logger;" : "")}
                    {(requestAwaiter.CheckCurrentState ? $"{_currentStateFunc()} = currentState;" : "")}
                    {(requestAwaiter.AfterCommit ? $"{_afterCommitFunc()} = afterCommit;" : "")}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                    {_inputTopicName(inputData)} = {inputData.NameCamelCase}Name;
                    {Partitions(inputData)} = {inputData.NameCamelCase}Partitions;
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                    {_outputTopicName(outputData)} = {outputTopicName(outputData)};
                    {_producerPool(outputData)} = producerPool{i};
");
                if(requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                    {_afterSendFunc(outputData)} = {afterSendFunc(outputData)};
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                    {_loadOutputFunc(outputData)} = {loadOutputFuncName(outputData)};
                    {_checkOutputStatusFunc(outputData)} = {checkOutputStatusFunc(outputData)};
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
                private readonly {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {_currentStateFunc()};
");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                private readonly {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {_afterCommitFunc()};
");
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                    private bool {_consumeCanceled(inputData)};
                    private TaskCompletionSource<List<Confluent.Kafka.TopicPartitionOffset>> {_tcsPartitions(inputData)};

                    private readonly string {_inputTopicName(inputData)};
                    public int[] {Partitions(inputData)} {{ get; init; }}
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                    private readonly string {_outputTopicName(outputData)};
                    private readonly {outputData.FullPoolInterfaceName} {_producerPool(outputData)};
");
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"private readonly {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {_afterSendFunc(outputData)};");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                    private readonly {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {_loadOutputFunc(outputData)};
                    private readonly {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {_checkOutputStatusFunc(outputData)};
");
                }
            }
        }

        private static void StopAsync(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async ValueTask StopAsync(CancellationToken token = default)
            {{
                var isEmpty = false;
                while(!isEmpty && (token == default || token.IsCancellationRequested))
                {{
                    {_lock()}.EnterReadLock();
                    try
                    {{
                        isEmpty |= {_responseAwaiters()}.Count == 0;
                    }}
                    finally
                    {{ 
                        {_lock()}.ExitReadLock();
                    }}

                    await Task.Delay(25);
                }}

                {_lock()}.EnterReadLock();
                try
                {{
                    foreach(var awaiter in {_responseAwaiters()}.Values)
                    {{
                        awaiter.Dispose(); 
                    }}
                }}
                finally
                {{ 
                    {_lock()}.ExitReadLock();
                }}

                await StopConsume();

                {_lock()}.Dispose();
                {_lock()} = null;
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
                {_consumeRoutines()} = new Thread[{requestAwaiter.InputDatas.Count}];
                {_ctsConsume()} = new CancellationTokenSource();
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                {_consumeRoutines()}[{i}] = Start{inputData.NamePascalCase}(bootstrapServers, groupId, changeConfig);
                {_consumeRoutines()}[{i}].Start();
                {_consumeCanceled(inputData)} = false;
                {_tcsPartitions(inputData)} = new();
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
                private Thread Start{inputData.NamePascalCase}(
                    string bootstrapServers,
                    string groupId,
                    Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                    )
                {{
                    return new Thread((param) =>
                    {{
                        start:

                        if({_ctsConsume()}.Token.IsCancellationRequested)
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

                            conf.GroupId = $""{{groupId}}Bucket{{{_bucketId()}}}"";
                            conf.BootstrapServers = bootstrapServers;
                            conf.AutoOffsetReset = AutoOffsetReset.Earliest;
                            conf.AllowAutoCreateTopics = false;
                            conf.EnableAutoCommit = false;

                            var consumer =
                                new ConsumerBuilder<{inputData.TypesPair}>(conf)
                                .Build()
                                ;

                            consumer.Assign({Partitions(inputData)}.Select(sel => new Confluent.Kafka.TopicPartition({_inputTopicName(inputData)}, sel)));
                            try
                            {{
                                var offsets = new Dictionary<Confluent.Kafka.Partition, Confluent.Kafka.TopicPartitionOffset>();
                                while (!{_ctsConsume()}.Token.IsCancellationRequested)
                                {{
                                    try
                                    {{
                                        ConsumeResult<{inputData.TypesPair}> consumeResult = consumer.Consume(30);
                                        try
                                        {{
                                            {_ctsConsume()}.Token.ThrowIfCancellationRequested();
                                        }}
                                        catch (OperationCanceledException oce)
                                        {{
                                            Volatile.Read(ref {_tcsPartitions(inputData)})?.TrySetCanceled();
                                            {_lock()}.EnterReadLock();
                                            try
                                            {{
                                                foreach (var topicResponseItem in {_responseAwaiters()}.Values)
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
                                                {_lock()}.ExitReadLock();
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

                                            var header = {assemblyName}.ResponseHeader.Parser.ParseFrom(infoBytes);
                                            if(header.Bucket != {_bucketId()})
                                            {{
                                                offsets[consumeResult.Partition] = consumeResult.TopicPartitionOffset;
                                                continue;
                                            }}

                                            inputMessage = new()
                                            {{
                                                {InputMessages.OriginalMessage()} = consumeResult.Message,
                                                {BaseInputMessage.Partition()} = consumeResult.Partition,
                                                {InputMessages.Header()} = header,
                                                {BaseInputMessage.TopicName()} = {_inputTopicName(inputData)}
                                            }};
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
                                            {LogInputMessage(requestAwaiter, inputData, "LogInformation")}
                                        }}
                                        while (true) 
                                        {{
                                            var locked = {_lock()}.TryEnterUpgradeableReadLock(15);
                                            if(locked)
                                            {{
                                                {TopicResponse.TypeFullName(requestAwaiter)} topicResponse = null;
                                                try
                                                {{
                                                    if (inputMessage != null)
                                                    {{
                                                        {_responseAwaiters()}.TryGetValue(inputMessage.Header.AnswerToMessageGuid, out topicResponse);
                                                    }}

                                                    if ({_addedCount()} == {_maxInFly()} && {_responseAwaiters()}.Count == 0)
                                                    {{
                                                        {_lock()}.EnterWriteLock();
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
                                                            Volatile.Write(ref {_tcsPartitions(inputDataNotSelf)}, tcsPartitions{j});
                                                            Volatile.Write(ref {_consumeCanceled(inputDataNotSelf)}, true);
                                                            var partitions{j} = tcsPartitions{j}.Task.Result;
                                                            allPartitions.AddRange(partitions{j});
");
                }
                builder.Append($@"
                                                            if(allPartitions.Count != 0)
                                                                consumer.Commit(allPartitions);
                                                            {_addedCount()} = 0;
                                                            {_fws()}.SignalFree();
");
                if(requestAwaiter.AfterCommit)
                {
                    builder.Append($@"
                                                            {_afterCommitFunc()}(
                                                                {_bucketId()},
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
                                                            {_lock()}.ExitWriteLock();
                                                        }}
                                                    }}
                                                }}
                                                finally
                                                {{
                                                    {_lock()}.ExitUpgradeableReadLock();
                                                }}

                                                if(topicResponse != null)
                                                {{
");
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                                                    topicResponse.TrySetResponse({inputData.Id}, inputMessage);
");
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
                                                            topicResponse.TrySetResponse({inputData.Id}, inputMessage, {acceptedServiceId});
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
                                            else if(Volatile.Read(ref {_consumeCanceled(inputData)}))
                                            {{
                                                Volatile.Write(ref {_consumeCanceled(inputData)}, false);
                                                Volatile.Read(ref {_tcsPartitions(inputData)}).SetResult(offsets.Values.ToList());
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
                                {(requestAwaiter.UseLogger ? @"_logger.LogError(agg);" : string.Empty)}
                                goto start;
                            }}
                        }}
                        catch(Exception {(requestAwaiter.UseLogger ? @"ex" : string.Empty)})
                        {{
                            {(requestAwaiter.UseLogger ? @"_logger.LogError(ex);" : string.Empty)}
                            goto start;
                        }}
                    }}
                )
                    {{
                        IsBackground = true,
                        Priority = ThreadPriority.AboveNormal,
                        Name = $""{{groupId}}Bucket{{{_bucketId()}}}{_inputTopicName(inputData)}""
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
                    {_ctsConsume()}?.Cancel();
");
            builder.Append($@"
                    {_lock()}.EnterWriteLock();
                    try
                    {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                        Volatile.Write(ref {_consumeCanceled(inputData)}, true);
");
            }
            builder.Append($@"
                    }}
                    finally
                    {{ 
                        {_lock()}.ExitWriteLock();
                    }}
                    
                    if({_consumeRoutines()} != null)//if not started
                    {{
                        foreach (var consumeRoutine in {_consumeRoutines()})
                        {{
                            while(consumeRoutine.IsAlive)
                            {{
                                await Task.Delay(50);
                            }}
                        }}
                    }}

                    {_ctsConsume()}?.Dispose();
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
                {TopicResponse.TypeFullName(requestAwaiter)} awaiter = null;

                var needDispose = false;
                if ({_lock()}.WaitingUpgradeCount == 0 && {_lock()}.TryEnterUpgradeableReadLock(10))
                {{
                    try
                    {{
                        if({_addedCount()} == {_maxInFly()})
                        {{
                            return new {TryDelayProduceResult.TypeFullName(requestAwaiter)} 
                            {{
                                {TryDelayProduceResult.Succsess()} = false 
                            }};
                        }}
                        else
                        {{
                            messageGuid = Guid.NewGuid().ToString(""D"");
                            awaiter = 
                                new {TopicResponse.TypeFullName(requestAwaiter)}(
                                        {_bucketId()},
                                        {(requestAwaiter.CheckCurrentState ? $"{_currentStateFunc()}," : "")}
                                        messageGuid,
                                        RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                    {Partitions(inputData)}
");
            }
            builder.Append($@",
                                        waitResponseTimeout
                                        );

                            {_lock()}.EnterWriteLock();
                            try
                            {{
                                if (!{_responseAwaiters()}.TryAdd(messageGuid, awaiter))
                                {{
                                    needDispose = true;
                                }}
                                else
                                {{
                                    {_addedCount()}++;
                                    if({_addedCount()} == {_maxInFly()})
                                    {{
                                        {_fws()}.SignalStuck();
                                    }}
                                }}
                            }}
                            finally
                            {{
                                {_lock()}.ExitWriteLock();
                            }}
                        }}
                    }}
                    finally
                    {{
                        {_lock()}.ExitUpgradeableReadLock();
                    }}
                }}
                else
                {{
                    return new {TryDelayProduceResult.TypeFullName(requestAwaiter)}
                    {{ 
                        {TryDelayProduceResult.Succsess()} = false 
                    }};
                }}

                if(needDispose)
                {{
                    awaiter.Dispose(); 
                    return new {TryDelayProduceResult.TypeFullName(requestAwaiter)}
                    {{
                        {TryDelayProduceResult.Succsess()} = false 
                    }};
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
                return new {TryDelayProduceResult.TypeFullName(requestAwaiter)}
                {{
                    {TryDelayProduceResult.Succsess()} = true, 
                    {TryDelayProduceResult.Response()} = awaiter, 
                    {TryDelayProduceResult.Bucket()} = this
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                    {TryDelayProduceResult.Header(outputData)} = header{i},
                    {TryDelayProduceResult.Message(outputData)} = 
                        new {OutputMessages.TypeName(outputData)}(
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
            public async ValueTask<{Response.TypeFullName(requestAwaiter)}> Produce(
                {TryDelayProduceResult.TypeFullName(requestAwaiter)} tryDelayProduce
                )
            {{
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                var variable = i == 0 ? "var producer" : "producer";
                builder.Append($@"
                {variable} = {_producerPool(outputData)}.Rent();
                try
                {{
                    var deliveryResult = await producer.ProduceAsync(
                        {_outputTopicName(outputData)},
                        tryDelayProduce.{TryDelayProduceResult.Message(outputData)}.{OutputMessages.Message()}
                        ).
                        ConfigureAwait(false)
                        ;
                }}
                catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.UseLogger ? "e" : "")})
                {{
                    {(requestAwaiter.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
                    {_lock()}.EnterWriteLock();
                    try
                    {{
                        {_responseAwaiters()}.Remove(tryDelayProduce.{TryDelayProduceResult.Response()}.{TopicResponse.MessageGuid()}, out _);
                    }}
                    finally
                    {{
                        {_lock()}.ExitWriteLock();
                    }}
                    tryDelayProduce.{TryDelayProduceResult.Response()}.Dispose();

                    throw;
                }}
                finally
                {{
                    {_producerPool(outputData)}.Return(producer);
                }}
");
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                await {_afterSendFunc(outputData)}(
                        tryDelayProduce.{TryDelayProduceResult.Header(outputData)},
                        tryDelayProduce.{TryDelayProduceResult.Message(outputData)}
                        )
                        .ConfigureAwait(false)
                        ;");
                }
            }
            builder.Append($@"
                return 
                    await tryDelayProduce.{TryDelayProduceResult.Response()}.GetResponse().ConfigureAwait(false);
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
                    $"async ValueTask<{TopicResponse.TypeFullName(requestAwaiter)}>" : 
                    $"{TopicResponse.TypeFullName(requestAwaiter)}"
                    ;
            builder.Append($@"
            public {returnType} AddAwaiter(
                string messageGuid,
                int waitResponseTimeout = 0
                )
            {{
                {TopicResponse.TypeFullName(requestAwaiter)} awaiter = null;
                
                var needDispose = false;
                {_lock()}.EnterUpgradeableReadLock();
                try
                {{
                    if ({_addedCount()} == {_maxInFly()})
                    {{
                        throw new InvalidOperationException(""Expect awaiter's limit exceeded"");
                    }}
                    else
                    {{
                        awaiter =
                            new {TopicResponse.TypeFullName(requestAwaiter)}(
                                    {_bucketId()},
                                    {(requestAwaiter.CheckCurrentState ? $"{_currentStateFunc()}," : "")}
                                    messageGuid,
                                    RemoveAwaiter
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                {Partitions(inputData)}
");
            }
            builder.Append($@",
                                    waitResponseTimeout
                                    );

                        {_lock()}.EnterWriteLock();
                        try
                        {{
                            if (!{_responseAwaiters()}.TryAdd(messageGuid, awaiter))
                            {{
                                needDispose = true;
                            }}
                            else
                            {{
                                {_addedCount()}++;
                                if({_addedCount()} == _maxInFly)
                                {{
                                    {_fws()}.SignalStuck();
                                }}
                            }}
                        }}
                        finally
                        {{
                            {_lock()}.ExitWriteLock();
                        }}
                    }}
                }}
                finally
                {{
                    {_lock()}.ExitUpgradeableReadLock();
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
                        await {_checkOutputStatusFunc(outputData)}(
                                messageGuid,
                                {_bucketId()}
");
                    for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                    {
                        var inputData = requestAwaiter.InputDatas[j];
                        builder.Append($@",
                                {Partitions(inputData)}
");
                    }
                    builder.Append($@"
                                )
                                .ConfigureAwait(false);

                    if(status < KafkaExchanger.Attributes.Enums.RAState.Sended)
                    {{
                        var message = 
                            await {_loadOutputFunc(outputData)}(
                                    messageGuid,
                                    {_bucketId()}
");
                    for (int j = 0; j < requestAwaiter.InputDatas.Count; j++)
                    {
                        var inputData = requestAwaiter.InputDatas[j];
                        builder.Append($@",
                                    {Partitions(inputData)}
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

                        var producer = {_producerPool(outputData)}.Rent();
                        try
                        {{
                            var deliveryResult = await producer.ProduceAsync(
                                    {_outputTopicName(outputData)},
                                    message.Message
                                    )
                                    .ConfigureAwait(false);
                        }}
                        catch (ProduceException<{outputData.TypesPair}> {(requestAwaiter.UseLogger ? "e" : "")})
                        {{
                            {(requestAwaiter.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
                            {_lock()}.EnterWriteLock();
                            try
                            {{
                                {_responseAwaiters()}.Remove(awaiter.MessageGuid, out _);
                            }}
                            finally
                            {{
                                {_lock()}.ExitWriteLock();
                            }}
                            awaiter.Dispose();

                            throw;
                        }}
                        finally
                        {{
                            {_producerPool(outputData)}.Return(producer);
                        }}
");
                    if (requestAwaiter.AfterSend)
                    {
                        builder.Append($@"
                        await {_afterSendFunc(outputData)}(header, message).ConfigureAwait(false);
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
                    {_lock()}.EnterWriteLock();
                    try
                    {{
                        if({_responseAwaiters()}.Remove(guid, out var value))
                        {{
                            value.Dispose();
                        }}
                    }}
                    finally
                    {{
                        {_lock()}.ExitWriteLock();
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
                var header = new {assemblyName}.RequestHeader()
                {{
                    Bucket = {_bucketId()}
                }};
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                var variable = i == 0 ? $"var topic" : $"topic";
                builder.Append($@"
                {variable} = new {assemblyName}.Topic()
                {{
                    Name = {_inputTopicName(inputData)}
                }};
                topic.Partitions.Add({Partitions(inputData)});
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
                header.TopicsForAnswer.Add(topic);
");

            }
            builder.Append($@"
                return header;
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

            return $@"_logger.{logMethod}($""Consumed inputMessage Key: '{{inputMessage.Key}}', Value: '{{inputMessage.Value}}'{afterMessageInfo}."");";
        }

        private static void End(StringBuilder builder)
        {
            builder.Append($@"
        }}
");
        }
    }
}