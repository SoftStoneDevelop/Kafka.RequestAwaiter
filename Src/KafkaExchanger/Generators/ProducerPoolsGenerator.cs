using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Datas;
using Microsoft.CodeAnalysis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection.Metadata;
using System.Text;

namespace KafkaExchanger.Generators
{
    internal class ProducerPoolsGenerator
    {
        StringBuilder _builder = new StringBuilder();
        HashSet<ProducerPair> _producers = new HashSet<ProducerPair>(ProducerPairComparer.Default);

        public void FillProducerTypes(
            List<RequestAwaiterData> requestAwaiterDatas,
            List<ResponderData> responderDatas
            ) 
        {
            foreach (var requestAwaiterData in requestAwaiterDatas) 
            {
                var pair = new ProducerPair(requestAwaiterData.OutcomeKeyType, requestAwaiterData.OutcomeValueType);
                _producers.Add(pair);
            }

            foreach (var responderData in responderDatas)
            {
                var pair = new ProducerPair(responderData.OutcomeKeyType, responderData.OutcomeValueType);
                _producers.Add(pair);
            }
        }

        public void GenerateProducerPools(SourceProductionContext context)
        {
            _builder.Clear();

            Start();
            foreach (var producerPair in _producers)
            {
                GenerateProducerPoolInterface(producerPair);
                GenerateProducerPool(producerPair);
            }

            End();

            context.AddSource($"ProducerPools.g.cs", _builder.ToString());
        }

        private void GenerateProducerPoolInterface(ProducerPair pair)
        {
            _builder.Append($@"
    public interface {pair.PoolInterfaceName}
    {{
        public IProducer<{pair.FullKeyTypeName}, {pair.FullValueTypeName}> Rent();

        public void Return(IProducer<{pair.FullKeyTypeName}, {pair.FullValueTypeName}> producer);
    }}
");
        }

        private void GenerateProducerPool(ProducerPair pair)
        {
            _builder.Append($@"
    public class ProducerPool{pair.KeyTypeAlias}{pair.ValueTypeAlias} : {pair.PoolInterfaceName}, System.IDisposable
    {{
        private readonly IProducer<{pair.FullKeyTypeName}, {pair.FullValueTypeName}>[] _producers;

        public ProducerPool{pair.KeyTypeAlias}{pair.ValueTypeAlias}(
            uint producerCount,
            string bootstrapServers
            )
        {{
            _producers = new IProducer<{pair.FullKeyTypeName}, {pair.FullValueTypeName}>[producerCount];
            var config = new ProducerConfig
            {{
                BootstrapServers = bootstrapServers,
                AllowAutoCreateTopics = false
            }};

            for (int i = 0; i < producerCount; i++)
            {{
                _producers[i] =
                    new ProducerBuilder<{pair.FullKeyTypeName}, {pair.FullValueTypeName}>(config)
                    .Build()
                    ;
            }}
        }}

        public IProducer<{pair.FullKeyTypeName}, {pair.FullValueTypeName}> Rent()
        {{
            return _producers[ChooseItemIndex()];
        }}

        public void Return(IProducer<{pair.FullKeyTypeName}, {pair.FullValueTypeName}> producer)
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
