using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KafkaExchengerTests2
{
    public interface IProducerPoolNullString
    {
        public Task Produce(string topic, Message<Confluent.Kafka.Null, System.String> message);

        public Task Produce(Confluent.Kafka.TopicPartition topicPartition, Message<Confluent.Kafka.Null, System.String> message);
    }

    public class ProducerPoolNullString : IProducerPoolNullString, System.IDisposable
    {
        private class ProduceInfo
        {
            public Message<Confluent.Kafka.Null, System.String> Message;
            public TaskCompletionSource CompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private class ByName : ProduceInfo
        {
            public string Name;
        }

        private class ByTopicPartition : ProduceInfo
        {
            public Confluent.Kafka.TopicPartition TopicPartition;
        }

        public ProducerPoolNullString(
            uint producerCount,
            string bootstrapServers,
            Action<Confluent.Kafka.ProducerConfig> changeConfig = null
            )
        {
            var config = new Confluent.Kafka.ProducerConfig();
            if (changeConfig != null)
            {
                changeConfig(config);
            }

            config.BootstrapServers = bootstrapServers;
            config.SocketTimeoutMs = 60000;
            config.TransactionTimeoutMs = 5000;
            _config = config;

            _routines = new Task[producerCount];
            for (int i = 0; i < producerCount; i++)
            {
                _routines[i] = ProduceRoutine(_cancellationTokenSource.Token);
            }
        }

        private Task[] _routines;
        private Confluent.Kafka.ProducerConfig _config;
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private Channel<ProduceInfo> _produceChannel = Channel.CreateUnbounded<ProduceInfo>(new UnboundedChannelOptions
        {
            AllowSynchronousContinuations = false,
            SingleReader = true,
            SingleWriter = false
        });

        private async Task ProduceRoutine(CancellationToken cancellationToken)
        {
            var reader = _produceChannel.Reader;
            var sendTemp = new List<ProduceInfo>(100);
            Confluent.Kafka.IProducer<Confluent.Kafka.Null, System.String> producer = null;
            start:
            try
            {
                producer =
                    new Confluent.Kafka.ProducerBuilder<Confluent.Kafka.Null, System.String>(_config)
                    .Build()
                    ;

                void sendPack()
                {
                    while (sendTemp.Count > 0)
                    {
                        producer.BeginTransaction();
                        for (int i = 0; i < sendTemp.Count; i++)
                        {
                            var sendInfo = sendTemp[i];
                            if (sendInfo is ByName byName)
                            {
                                producer.Produce(byName.Name, byName.Message);
                            }
                            else if (sendInfo is ByTopicPartition byTopicPartition)
                            {
                                producer.Produce(byTopicPartition.TopicPartition, byTopicPartition.Message);
                            }
                        }

                        try
                        {
                            while (true)
                            {
                                try
                                {
                                    producer.CommitTransaction();
                                }
                                catch (KafkaRetriableException)
                                {
                                    continue;
                                }

                                break;
                            }
                        }
                        catch (KafkaTxnRequiresAbortException)
                        {
                            producer.AbortTransaction();
                            continue;
                        }

                        for (int i = 0;i < sendTemp.Count; i++)
                        {
                            var sended = sendTemp[i];
                            sended.CompletionSource.SetResult();
                        }

                        sendTemp.Clear();
                    }
                }

                sendPack();
                while (!cancellationToken.IsCancellationRequested)
                {                    
                    var info = await reader.ReadAsync(cancellationToken);
                    var sw = Stopwatch.StartNew();
                    sendTemp.Add(info);
                    while ((sw.ElapsedMilliseconds < 1 || sendTemp.Count == 100) && reader.TryRead(out info))
                    {
                        sendTemp.Add(info);
                    }
                }
            }
            catch
            {
                //ignore
            }
            finally
            {
                try
                {
                    producer?.Dispose();
                }
                catch
                {
                    //ignore
                }
            }

            if(!cancellationToken.IsCancellationRequested)
            {
                goto start;
            }

            for (int i = 0; i < sendTemp.Count; i++)
            {
                var sended = sendTemp[i];
                sended.CompletionSource.SetCanceled(cancellationToken);
            }
        }

        public async Task Produce(string topic, Message<Null, string> message)
        {
            var info = new ByName
            {
                Message = message,
                Name = topic
            };

            await _produceChannel.Writer.WriteAsync(info).ConfigureAwait(false);
            await info.CompletionSource.Task.ConfigureAwait(false);
        }

        public async Task Produce(TopicPartition topicPartition, Message<Null, string> message)
        {
            var info = new ByTopicPartition
            {
                Message = message,
                TopicPartition = topicPartition
            };

            await _produceChannel.Writer.WriteAsync(info).ConfigureAwait(false);
            await info.CompletionSource.Task.ConfigureAwait(false);
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();

            _produceChannel.Writer.Complete();

            _cancellationTokenSource.Dispose();
        }
    }
}