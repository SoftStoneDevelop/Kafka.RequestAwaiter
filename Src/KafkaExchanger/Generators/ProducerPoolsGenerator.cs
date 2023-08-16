using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Datas;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators
{
    internal class ProducerPoolsGenerator
    {
        StringBuilder _builder = new StringBuilder();
        HashSet<OutputData> _producers = new HashSet<OutputData>(ProducerPairComparer.Default);

        public void FillProducerTypes(
            List<KafkaExchanger.AttributeDatas.RequestAwaiter> requestAwaiters,
            List<Responder> responders
            ) 
        {
            foreach (var requestAwaiter in requestAwaiters) 
            {
                foreach (var outputData in requestAwaiter.OutputDatas)
                {
                    _producers.Add(outputData);
                }
            }

            foreach (var responder in responders)
            {
                foreach (var outputData in responder.OutputDatas)
                {
                    _producers.Add(outputData);
                }
            }
        }

        public void GenerateProducerPools(SourceProductionContext context)
        {
            _builder.Clear();

            Start();
            foreach (var outputData in _producers)
            {
                GenerateProducerPoolInterface(outputData);
                GenerateProducerPool(outputData);
            }

            End();

            context.AddSource($"ProducerPools.g.cs", _builder.ToString());
        }

        private void GenerateProducerPoolInterface(OutputData outputData)
        {
            _builder.Append($@"
    public interface {outputData.PoolInterfaceName}
    {{
        public IProducer<{outputData.FullKeyTypeName}, {outputData.FullValueTypeName}> Rent();

        public void Return(IProducer<{outputData.FullKeyTypeName}, {outputData.FullValueTypeName}> producer);
    }}
");
        }

        private void GenerateProducerPool(OutputData outputData)
        {
            _builder.Append($@"
    public class ProducerPool{outputData.KeyTypeAlias}{outputData.ValueTypeAlias} : {outputData.PoolInterfaceName}, System.IDisposable
    {{
        private readonly IProducer<{outputData.FullKeyTypeName}, {outputData.FullValueTypeName}>[] _producers;

        public ProducerPool{outputData.KeyTypeAlias}{outputData.ValueTypeAlias}(
            uint producerCount,
            string bootstrapServers,
            Action<Confluent.Kafka.ProducerConfig> changeConfig = null
            )
        {{
            _producers = new IProducer<{outputData.FullKeyTypeName}, {outputData.FullValueTypeName}>[producerCount];
            var config = new Confluent.Kafka.ProducerConfig();
            if(changeConfig != null)
            {{
                changeConfig(config);
            }}

            config.BootstrapServers = bootstrapServers;

            for (int i = 0; i < producerCount; i++)
            {{
                _producers[i] =
                    new ProducerBuilder<{outputData.FullKeyTypeName}, {outputData.FullValueTypeName}>(config)
                    .Build()
                    ;
            }}
        }}

        public IProducer<{outputData.FullKeyTypeName}, {outputData.FullValueTypeName}> Rent()
        {{
            return _producers[ChooseItemIndex()];
        }}

        public void Return(IProducer<{outputData.FullKeyTypeName}, {outputData.FullValueTypeName}> producer)
        {{
            //nothing
        }}

        private uint _currentItemIndex = 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private uint ChooseItemIndex()
        {{
            var index = Interlocked.Increment(ref _currentItemIndex);
            return index % (uint)_producers.Length;
        }}

        public void Dispose()
        {{
            foreach (var producer in _producers)
            {{
                producer.Dispose();
            }}
        }}
    }}
");
        }

        private void Start()
        {
            _builder.Append($@"
using Confluent.Kafka;
using System.Runtime.CompilerServices;
using System.Threading;
using System;

namespace KafkaExchanger.Common
{{
");
        }

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}
