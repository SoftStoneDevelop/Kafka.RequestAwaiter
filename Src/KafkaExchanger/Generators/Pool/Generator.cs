using KafkaExchanger.Datas;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Pool
{
    internal class Generator
    {
        StringBuilder _builder = new StringBuilder();
        HashSet<OutputData> _outputs = new HashSet<OutputData>(ProducerPairComparer.Default);

        public void FillProducerTypes(
            List<Datas.RequestAwaiter> requestAwaiters,
            List<Datas.Responder> responders
            )
        {
            foreach (var requestAwaiter in requestAwaiters)
            {
                foreach (var outputData in requestAwaiter.OutputDatas)
                {
                    _outputs.Add(outputData);
                }
            }

            foreach (var responder in responders)
            {
                foreach (var outputData in responder.OutputDatas)
                {
                    _outputs.Add(outputData);
                }
            }
        }

        public void Generate(
            string assemblyName,
            SourceProductionContext context
            )
        {
            _builder.Clear();

            Start(assemblyName);
            foreach (var outputData in _outputs)
            {
                Interface.Append(_builder, outputData);
                Pool.Append(_builder, assemblyName, outputData);
            }

            End();

            context.AddSource($"ProducerPools.g.cs", _builder.ToString());
            _outputs.Clear();
            _builder.Clear();
        }

        private void Start(string assemblyName)
        {
            _builder.Append($@"
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

namespace {assemblyName}
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
