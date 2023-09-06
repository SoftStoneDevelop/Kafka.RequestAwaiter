using KafkaExchanger.Datas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class RequestAwaiter
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            StartClass(builder, requestAwaiter);

            //inner classes
            DelayProduce.Append(builder, assemblyName, requestAwaiter);

            Response.Append(builder, assemblyName, requestAwaiter);

            Config.Append(builder);
            ProcessorConfig.Append(builder, assemblyName, requestAwaiter);
            ConsumerInfo.Append(builder);
            ProducerInfo.Append(builder);

            BaseInputMessage.Append(builder, assemblyName, requestAwaiter);
            InputMessages.Append(builder, assemblyName, requestAwaiter);

            OutputMessage.Append(builder, assemblyName, requestAwaiter);
            OutputMessages.Append(builder, assemblyName, requestAwaiter);

            TryAddAwaiterResult.Append(builder, requestAwaiter);
            TopicResponse.Append(builder, assemblyName, requestAwaiter);

            ChannelInfo.Append(builder, assemblyName, requestAwaiter);
            StartResponse.Append(builder, assemblyName, requestAwaiter);
            SetOffsetResponse.Append(builder, assemblyName, requestAwaiter);
            EndResponse.Append(builder, assemblyName, requestAwaiter);

            PartitionItem.Append(builder, assemblyName, requestAwaiter);

            //methods
            StartMethod(builder, requestAwaiter);
            Setup(builder, requestAwaiter);
            Produce(builder, requestAwaiter);
            ProduceDelay(builder, assemblyName, requestAwaiter);
            AddAwaiter(builder, requestAwaiter);
            StopAsync(builder);
            DisposeAsync(builder);

            EndClass(builder);
        }

        private static string _fws()
        {
            return "_fws";
        }

        private static string _items()
        {
            return "_items";
        }

        private static void StartClass(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
    {requestAwaiter.TypeSymbol.DeclaredAccessibility.ToName()} partial class {requestAwaiter.TypeSymbol.Name} : I{requestAwaiter.TypeSymbol.Name}RequestAwaiter
    {{
        {(requestAwaiter.UseLogger ? @"private readonly ILoggerFactory _loggerFactory;" : "")}
        private {PartitionItem.TypeFullName(requestAwaiter)}[] {_items()};
        private string _bootstrapServers;
        private string _groupId;
        private volatile bool _isRun;
        private KafkaExchanger.FreeWatcherSignal {_fws()};

        public {requestAwaiter.TypeSymbol.Name}({(requestAwaiter.UseLogger ? @"ILoggerFactory loggerFactory" : "")})
        {{
            {(requestAwaiter.UseLogger ? @"_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private static void StartMethod(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public void Start(
            Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
            )
        {{
            if(_isRun)
            {{
                throw new System.Exception(""Before starting, you need to stop the previous run: call StopAsync"");
            }}

            _isRun = true;
            foreach (var item in {_items()})
            {{
                item.Start(
                    _bootstrapServers,
                    _groupId,
                    changeConfig
                    );
            }}
        }}
");
        }

        private static void EndClass(StringBuilder builder)
        {
            builder.Append($@"
    }}
");
        }

        private static void Setup(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public void Setup(
            {requestAwaiter.TypeSymbol.Name}.Config config
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@",
            {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            builder.Append($@"
            )
        {{
            if({_items()} != null)
            {{
                throw new System.Exception(""Before setup new configuration, you need to stop the previous: call StopAsync"");
            }}

            {_items()} = new {PartitionItem.TypeFullName(requestAwaiter)}[config.{Config.Processors()}.Length];
            _bootstrapServers = config.{Config.BootstrapServers()};
            _groupId = config.{Config.GroupId()};
            {_fws()} = new(config.{Config.Processors()}.Sum(s => s.{ProcessorConfig.Buckets()}));
            for (int i = 0; i < config.{Config.Processors()}.Length; i++)
            {{
                var processorConfig = config.{Config.Processors()}[i];
                {_items()}[i] =
                    new {PartitionItem.TypeFullName(requestAwaiter)}(
                        {_fws()}");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                        processorConfig.{ProcessorConfig.ConsumerInfo(inputData)}.{ConsumerInfo.TopicName()},
                        processorConfig.{ProcessorConfig.ConsumerInfo(inputData)}.{ConsumerInfo.Partitions()}");
            }
            builder.Append($@",
                        processorConfig.{ProcessorConfig.Buckets()},
                        processorConfig.{ProcessorConfig.MaxInFly()}");

            if(requestAwaiter.UseLogger)
            {
                builder.Append($@",
                        _loggerFactory.CreateLogger(config.GroupId)");
            }

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@",
                        processorConfig.{ProcessorConfig.CurrentState()}");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@",
                        processorConfig.{ProcessorConfig.AfterCommit()}");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@",
                        processorConfig.{ProcessorConfig.ProducerInfo(outputData)}.TopicName,
                        producerPool{i}");
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",
                        processorConfig.{ProcessorConfig.AfterSend(outputData)}");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                        processorConfig.{ProcessorConfig.LoadOutput(outputData)},
                        processorConfig.{ProcessorConfig.CheckOutputStatus(outputData)}");
                }
            }

            builder.Append($@"
                        );
            }}
        }}
");
        }

        private static void StopAsync(StringBuilder builder)
        {
            builder.Append($@"
        public async ValueTask StopAsync(CancellationToken token = default)
        {{
            var items = {_items()};
            if(items == null)
            {{
                return;
            }}

            {_items()} = null;
            _bootstrapServers = null;
            _groupId = null;

            var disposeTasks = new Task[items.Length];
            for (var i = 0; i < items.Length; i++)
            {{
                disposeTasks[i] = items[i].StopAsync(token).AsTask();
            }}
            
            await Task.WhenAll(disposeTasks);
            _isRun = false;
        }}
");
        }

        private static void Produce(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public async ValueTask<{Response.TypeFullName(requestAwaiter)}> Produce(
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

            while(true)
            {{
                var waitFree = {_fws()}.WaitFree();
                await waitFree.ConfigureAwait(false);
                for (int i = 0; i < {_items()}.Length; i++)
                {{
                    var index = Interlocked.Increment(ref _currentItemIndex) % (uint){_items()}.Length;
                    var item = {_items()}[index];
                    var tp =
                        item.TryProduceDelay(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                        key{i},
");
                }

                builder.Append($@"
                        value{i},
");
            }
            builder.Append($@"
                        waitResponseTimeout
                    );

                    if(tp.Succsess)
                    {{
                        return await tp.Bucket.Produce(tp);
                    }}
                }}

                waitFree = {_fws()}.WaitFree();
                await waitFree.ConfigureAwait(false);
            }}
        }}
        private uint _currentItemIndex = 0;
");
        }

        private static void ProduceDelay(
            StringBuilder builder,
            string assemblyName, 
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public async ValueTask<{DelayProduce.TypeFullName(requestAwaiter)}> ProduceDelay(
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

            while(true)
            {{
                var waitFree = {_fws()}.WaitFree();
                await waitFree.ConfigureAwait(false);
                for (int i = 0; i < {_items()}.Length; i++)
                {{
                    var index = Interlocked.Increment(ref _currentItemIndex) % (uint){_items()}.Length;
                    var item = {_items()}[index];
                    var tp =
                        item.TryProduceDelay(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
                {
                    builder.Append($@"
                            key{i},
");
                }

                builder.Append($@"
                            value{i},
");
            }
            builder.Append($@"
                            waitResponseTimeout
                    );

                    if(tp.Succsess)
                    {{
                        return new {requestAwaiter.TypeSymbol.Name}.DelayProduce(tp);
                    }}
                }}

                waitFree = {_fws()}.WaitFree();
                await waitFree.ConfigureAwait(false);
            }}
        }}
");
        }

        private static void AddAwaiter(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public async ValueTask<{Response.TypeFullName(requestAwaiter)}> AddAwaiter(
            string messageGuid,
            int bucket,
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
            int[] input{i}partitions,
");
            }
            builder.Append($@"
            int waitResponseTimeout = 0
            )
        {{

            for (var i = 0; i < {_items()}.Length; i++ )
            {{
                var taw =
                    {(requestAwaiter.AddAwaiterCheckStatus ? "await " : "")}{_items()}[i].TryAddAwaiter(
                        messageGuid,
                        bucket,
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                        input{i}partitions,
");
            }
            builder.Append($@"
                        waitResponseTimeout
                        );

                if(taw.Succsess)
                {{
                    return await taw.Response.GetResponse().ConfigureAwait(false);
                }}
            }}

            throw new System.Exception(""No matching bucket found in combination with partitions"");
        }}
");
        }

        private static void DisposeAsync(StringBuilder builder)
        {
            builder.Append($@"
        public async ValueTask DisposeAsync()
        {{
            await StopAsync();
        }}
");
        }
    }
}