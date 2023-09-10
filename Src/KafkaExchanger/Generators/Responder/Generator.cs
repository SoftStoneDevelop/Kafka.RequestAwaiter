using KafkaExchanger.Datas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;
using System;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal class Generator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateResponder(string assemblyName, KafkaExchanger.Datas.Responder responder, SourceProductionContext context)
        {
            _builder.Clear();

            Start(responder);

            Interface.Append(assemblyName, responder, _builder);
            Responder.Append(assemblyName, responder, _builder);

            End();

            context.AddSource($"{responder.TypeSymbol.Name}Responder.g.cs", _builder.ToString());
        }

        private void Start(KafkaExchanger.Datas.Responder responder)
        {
            _builder.Append($@"
using Confluent.Kafka;
using Google.Protobuf;
{(responder.UseLogger ? @"using Microsoft.Extensions.Logging;" : "")}
using System.Collections.Concurrent;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace {responder.TypeSymbol.ContainingNamespace}
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