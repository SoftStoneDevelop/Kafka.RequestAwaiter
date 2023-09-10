using Microsoft.CodeAnalysis;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal class Generator
    {
        StringBuilder _builder = new StringBuilder(1300);

        public void Generate(
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter,
            SourceProductionContext context
            )
        {
            _builder.Clear();

            Start(requestAwaiter);//start file

            Interface.Append(_builder, assemblyName, requestAwaiter);
            RequestAwaiter.Append(_builder, assemblyName, requestAwaiter);

            End();//end file

            context.AddSource($"{requestAwaiter.TypeSymbol.Name}RequesterAwaiter.g.cs", _builder.ToString());
        }

        private void Start(Datas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
using Confluent.Kafka;
{(requestAwaiter.UseLogger ? @"using Microsoft.Extensions.Logging;" : "")}
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using Google.Protobuf;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace {requestAwaiter.TypeSymbol.ContainingNamespace}
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