using KafkaExchanger.Datas;
using KafkaExchanger.Generators.RequestAwaiter;
using System.Reflection;
using System.Text;
using System.Threading;

namespace KafkaExchanger.Generators.Pool
{
    internal static class Pool
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {

            Start(builder, assemblyName, outputData);

            ProduceInfo.Append(builder, assemblyName, outputData);
            ByName.Append(builder, assemblyName, outputData);
            ByTopicPartition.Append(builder, assemblyName, outputData);

            Constructor(builder, outputData);
            PropertiesAndFields(builder, assemblyName, outputData);

            ProduceRoutine(builder, assemblyName, outputData);
            ProduceByName(builder, assemblyName, outputData);
            ProduceByPartition(builder, assemblyName, outputData);

            Dispose(builder, outputData);

            End(builder, outputData);
        }

        public static string TypeFullName(
            string assemblyName,
            OutputData outputData
            )
        {
            return $"{assemblyName}.{TypeName(outputData)}";
        }

        public static string TypeName(OutputData outputData)
        {
            return $"ProducerPool{outputData.KeyTypeAlias}{outputData.ValueTypeAlias}";
        }

        public static string _produceChannel()
        {
            return $"_produceChannel";
        }

        public static string _config()
        {
            return $"_config";
        }

        public static string _cancellationTokenSource()
        {
            return $"_cancellationTokenSource";
        }

        public static string _routines()
        {
            return $"_routines";
        }

        public static string _messagesInTransaction()
        {
            return $"_messagesInTransaction";
        }

        private static void Start(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {
            builder.Append($@"
    public partial class {TypeName(outputData)} : {Interface.TypeFullName(assemblyName, outputData)}, System.IDisposable
    {{
");
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {
            builder.Append($@"
        private int {_messagesInTransaction()};
        private Task[] {_routines()};
        private Confluent.Kafka.ProducerConfig {_config()};
        private CancellationTokenSource {_cancellationTokenSource()} = new CancellationTokenSource();
        private Channel<{ProduceInfo.TypeFullName(assemblyName, outputData)}> {_produceChannel()} = Channel.CreateUnbounded<{ProduceInfo.TypeFullName(assemblyName, outputData)}>(
            new UnboundedChannelOptions
            {{
                AllowSynchronousContinuations = false,
                SingleReader = true,
                SingleWriter = false
            }});
");
        }

        private static void Constructor(
            StringBuilder builder,
            OutputData outputData
            )
        {
            builder.Append($@"
        public {TypeName(outputData)}(
            uint producerCount,
            string bootstrapServers,
            int messagesInTransaction = 100,
            Action<Confluent.Kafka.ProducerConfig> changeConfig = null
            )
        {{
            {_messagesInTransaction()} = messagesInTransaction;
            var config = new Confluent.Kafka.ProducerConfig();
            config.SocketTimeoutMs = 60000;
            config.TransactionTimeoutMs = 5000;

            if (changeConfig != null)
            {{
                changeConfig(config);
            }}

            config.BootstrapServers = bootstrapServers;
            {_config()} = config;

            {_routines()} = new Task[producerCount];
            for (int i = 0; i < producerCount; i++)
            {{
                {_routines()}[i] = ProduceRoutine({_cancellationTokenSource()}.Token);
            }}
        }}
");
        }

        private static void ProduceRoutine(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {
            builder.Append($@"
        private async Task ProduceRoutine(CancellationToken cancellationToken)
        {{
            var reader = {_produceChannel()}.Reader;
            var sendTemp = new List<{ProduceInfo.TypeFullName(assemblyName, outputData)}>({_messagesInTransaction()});
            Confluent.Kafka.IProducer<{outputData.TypesPair}> producer = null;
            start:
            try
            {{
                producer =
                    new Confluent.Kafka.ProducerBuilder<{outputData.TypesPair}>({_config()})
                    .Build()
                    ;

                void sendPack()
                {{
                    while (sendTemp.Count > 0)
                    {{
                        producer.BeginTransaction();
                        for (int i = 0; i < sendTemp.Count; i++)
                        {{
                            var sendInfo = sendTemp[i];
                            if (sendInfo is {ByName.TypeFullName(assemblyName, outputData)} byName)
                            {{
                                producer.Produce(byName.{ByName.Name()}, byName.{ProduceInfo.Message()});
                            }}
                            else if (sendInfo is {ByTopicPartition.TypeFullName(assemblyName, outputData)} byTopicPartition)
                            {{
                                producer.Produce(byTopicPartition.{ByTopicPartition.TopicPartition()}, byTopicPartition.{ProduceInfo.Message()});
                            }}
                        }}

                        try
                        {{
                            while (true)
                            {{
                                try
                                {{
                                    producer.CommitTransaction();
                                }}
                                catch (KafkaRetriableException)
                                {{
                                    continue;
                                }}

                                break;
                            }}
                        }}
                        catch (KafkaTxnRequiresAbortException)
                        {{
                            producer.AbortTransaction();
                            continue;
                        }}

                        for (int i = 0;i < sendTemp.Count; i++)
                        {{
                            var sended = sendTemp[i];
                            sended.{ProduceInfo.CompletionSource()}.SetResult();
                        }}

                        sendTemp.Clear();
                    }}
                }}

                sendPack();
                while (!cancellationToken.IsCancellationRequested)
                {{                    
                    var info = await reader.ReadAsync(cancellationToken);
                    var sw = Stopwatch.StartNew();
                    sendTemp.Add(info);
                    while ((sw.ElapsedMilliseconds < 1 && sendTemp.Count < {_messagesInTransaction()}) && reader.TryRead(out info))
                    {{
                        sendTemp.Add(info);
                    }}
                }}
            }}
            catch
            {{
                //ignore
            }}
            finally
            {{
                try
                {{
                    producer?.Dispose();
                }}
                catch
                {{
                    //ignore
                }}
            }}

            if(!cancellationToken.IsCancellationRequested)
            {{
                goto start;
            }}

            for (int i = 0; i < sendTemp.Count; i++)
            {{
                var sended = sendTemp[i];
                sended.{ProduceInfo.CompletionSource()}.SetCanceled(cancellationToken);
            }}
        }}
");
        }

        private static void ProduceByName(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {
            builder.Append($@"
        public async Task Produce(string topicName, Confluent.Kafka.Message<{outputData.TypesPair}> message)
        {{
            var info = new {ByName.TypeFullName(assemblyName, outputData)}
            {{
                {ProduceInfo.Message()} = message,
                {ByName.Name()} = topicName
            }};

            await {_produceChannel()}.Writer.WriteAsync(info).ConfigureAwait(false);
            await info.{ProduceInfo.CompletionSource()}.Task.ConfigureAwait(false);
        }}
");
        }

        private static void ProduceByPartition(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {
            builder.Append($@"
        public async Task Produce(Confluent.Kafka.TopicPartition topicPartition, Confluent.Kafka.Message<{outputData.TypesPair}> message)
        {{
            var info = new {ByTopicPartition.TypeFullName(assemblyName, outputData)}
            {{
                {ProduceInfo.Message()} = message,
                {ByTopicPartition.TopicPartition()} = topicPartition
            }};

            await {_produceChannel()}.Writer.WriteAsync(info).ConfigureAwait(false);
            await info.{ProduceInfo.CompletionSource()}.Task.ConfigureAwait(false);
        }}
");
        }

        private static void Dispose(
            StringBuilder builder,
            OutputData outputData
            )
        {
            builder.Append($@"
        public void Dispose()
        {{
            {_cancellationTokenSource()}.Cancel();

            {_produceChannel()}.Writer.Complete();

            {_cancellationTokenSource()}.Dispose();
        }}
");
        }

        private static void End(
            StringBuilder builder,
            OutputData outputData
            )
        {
            builder.Append($@"
    }}
");
        }
    }
}
