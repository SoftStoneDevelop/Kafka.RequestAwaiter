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
    public partial class {TypeName(outputData)} : {Interface.TypeFullName(assemblyName, outputData)}, System.IAsyncDisposable
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
        private CancellationTokenSource {_cancellationTokenSource()} = new CancellationTokenSource();
        private Channel<{ProduceInfo.TypeFullName(assemblyName, outputData)}> {_produceChannel()} = Channel.CreateUnbounded<{ProduceInfo.TypeFullName(assemblyName, outputData)}>(
            new UnboundedChannelOptions
            {{
                AllowSynchronousContinuations = false,
                SingleReader = false,
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
            HashSet<string> transactionalIds,
            string bootstrapServers,
            int messagesInTransaction = 100,
            Action<Confluent.Kafka.ProducerConfig> changeConfig = null
            )
        {{
            {_messagesInTransaction()} = messagesInTransaction;
            {_routines()} = new Task[transactionalIds.Count];
            var i = 0;
            foreach (var transactionalId in transactionalIds)
            {{
                var config = new Confluent.Kafka.ProducerConfig();
                config.SocketTimeoutMs = 5000;
                config.TransactionTimeoutMs = 5000;

                if (changeConfig != null)
                {{
                    changeConfig(config);
                }}

                config.BootstrapServers = bootstrapServers;
                config.TransactionalId = transactionalId;
                {_routines()}[i++] = ProduceRoutine(config, {_cancellationTokenSource()}.Token);
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
        private async Task ProduceRoutine(Confluent.Kafka.ProducerConfig config, CancellationToken cancellationToken)
        {{
            var reader = {_produceChannel()}.Reader;
            var sendTemp = new List<{ProduceInfo.TypeFullName(assemblyName, outputData)}>({_messagesInTransaction()});
            var producer =
                new Confluent.Kafka.ProducerBuilder<{outputData.TypesPair}>(config)
                .Build()
                ;

            try
            {{
                producer.InitTransactions(TimeSpan.FromSeconds(60));
                while (!cancellationToken.IsCancellationRequested)
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

                    var info = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    var sw = Stopwatch.StartNew();
                    sendTemp.Add(info);
                    while (!cancellationToken.IsCancellationRequested && sw.ElapsedMilliseconds < 1 && sendTemp.Count < {_messagesInTransaction()} && reader.TryRead(out info))
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
                producer.Dispose();
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
        public async ValueTask DisposeAsync()
        {{
            {_cancellationTokenSource()}.Cancel();

            {_produceChannel()}.Writer.Complete();
            for (int i = 0; i < {_routines()}.Length; i++)
            {{
                try
                {{
                    await {_routines()}[i];
                }}
                catch
                {{
                    //ignore
                }}
            }}
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
