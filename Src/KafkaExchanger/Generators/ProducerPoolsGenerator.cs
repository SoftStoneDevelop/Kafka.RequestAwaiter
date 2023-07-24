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
        HashSet<OutcomeData> _producers = new HashSet<OutcomeData>(ProducerPairComparer.Default);

        public void FillProducerTypes(
            List<KafkaExchanger.AttributeDatas.GenerateData> requestAwaiters,
            List<Responder> responders
            ) 
        {
            foreach (var requestAwaiter in requestAwaiters) 
            {
                foreach (var outcomeData in requestAwaiter.OutcomeDatas)
                {
                    _producers.Add(outcomeData);
                }
            }

            foreach (var responder in responders)
            {
                foreach (var outcomeData in responder.OutcomeDatas)
                {
                    _producers.Add(outcomeData);
                }
            }
        }

        public void GenerateProducerPools(SourceProductionContext context)
        {
            _builder.Clear();

            Start();
            foreach (var outcomeData in _producers)
            {
                GenerateProducerPoolInterface(outcomeData);
                GenerateProducerPool(outcomeData);
            }

            End();

            context.AddSource($"ProducerPools.g.cs", _builder.ToString());
        }

        private void GenerateProducerPoolInterface(OutcomeData outcomeData)
        {
            _builder.Append($@"
    public interface {outcomeData.PoolInterfaceName}
    {{
        public IProducer<{outcomeData.FullKeyTypeName}, {outcomeData.FullValueTypeName}> Rent();

        public void Return(IProducer<{outcomeData.FullKeyTypeName}, {outcomeData.FullValueTypeName}> producer);
    }}
");
        }

        private void GenerateProducerPool(OutcomeData outcomeData)
        {
            _builder.Append($@"
    public class ProducerPool{outcomeData.KeyTypeAlias}{outcomeData.ValueTypeAlias} : {outcomeData.PoolInterfaceName}, System.IDisposable
    {{
        private readonly IProducer<{outcomeData.FullKeyTypeName}, {outcomeData.FullValueTypeName}>[] _producers;

        public ProducerPool{outcomeData.KeyTypeAlias}{outcomeData.ValueTypeAlias}(
            uint producerCount,
            string bootstrapServers
            )
        {{
            _producers = new IProducer<{outcomeData.FullKeyTypeName}, {outcomeData.FullValueTypeName}>[producerCount];
            var config = new ProducerConfig
            {{
                BootstrapServers = bootstrapServers,
                AllowAutoCreateTopics = false
            }};

            for (int i = 0; i < producerCount; i++)
            {{
                _producers[i] =
                    new ProducerBuilder<{outcomeData.FullKeyTypeName}, {outcomeData.FullValueTypeName}>(config)
                    .Build()
                    ;
            }}
        }}

        public IProducer<{outcomeData.FullKeyTypeName}, {outcomeData.FullValueTypeName}> Rent()
        {{
            return _producers[ChooseItemIndex()];
        }}

        public void Return(IProducer<{outcomeData.FullKeyTypeName}, {outcomeData.FullValueTypeName}> producer)
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
