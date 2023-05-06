using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Runtime.InteropServices.ComTypes;
using System.Text;

namespace KafkaExchanger
{
    [Generator]
    public partial class Genarator : ISourceGenerator
    {
        public void Execute(GeneratorExecutionContext context)
        {
            //System.Diagnostics.Debugger.Launch();

            var processor = new Processor();

            var c = (CSharpCompilation)context.Compilation;
            processor.FillTypes(c.Assembly.GlobalNamespace);
            processor.Generate(context);
        }

        public void Initialize(GeneratorInitializationContext context)
        {
            // No initialization required for this one
        }
    }
}