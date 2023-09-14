using KafkaExchanger.Generators;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Xml.Linq;

namespace KafkaExchanger
{
    [Generator]
    public class Generator : IIncrementalGenerator
    {
        public class ByArrayComparer : IEqualityComparer<(Compilation compilation, ImmutableArray<ClassDeclarationSyntax> Nodes)>
        {
            public bool Equals(
               (Compilation compilation, ImmutableArray<ClassDeclarationSyntax> Nodes) x,
               (Compilation compilation, ImmutableArray<ClassDeclarationSyntax> Nodes) y)
            {
                return x.Nodes.Equals(y.Nodes);
            }

            public int GetHashCode((Compilation compilation, ImmutableArray<ClassDeclarationSyntax> Nodes) obj)
            {
                return obj.Nodes.GetHashCode();
            }
        }

        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            //System.Diagnostics.Debugger.Launch();
            var classDeclarations = context.SyntaxProvider
                .CreateSyntaxProvider(
                predicate: (s, _) => IsSyntaxTargetForGeneration(s),
                transform: (ctx, _) => GetSemanticClass(ctx))
                .Where(m => m != null)
                .Collect()
                .Select((sel, _) => sel.Distinct().ToImmutableArray())
                ;

            var compilationAndClasses =
                context.CompilationProvider.Combine(classDeclarations)
                .WithComparer(new ByArrayComparer())
                ;

            context.RegisterSourceOutput(compilationAndClasses,
                (spc, source) => Execute(source.Item1, source.Item2, spc));
        }

        static bool IsSyntaxTargetForGeneration(SyntaxNode node)
        {
            if (!(node is ClassDeclarationSyntax classDeclarationSyntax))
            {
                return false;
            }

            var isPartial = false;
            foreach (var syntaxToken in classDeclarationSyntax.Modifiers)
            {
                if(syntaxToken.IsKind(SyntaxKind.StaticKeyword))
                {
                    return false;
                }

                isPartial |= syntaxToken.IsKind(SyntaxKind.PartialKeyword);
            }

            if(!isPartial)
            {
                return false;
            }

            if (classDeclarationSyntax.AttributeLists.Count == 0)
            {
                return false;
            }

            return true;
        }

        static ClassDeclarationSyntax GetSemanticClass(GeneratorSyntaxContext context)
        {
            var classDeclarationSyntax = (ClassDeclarationSyntax)context.Node;
            foreach (var attributeListSyntax in classDeclarationSyntax.AttributeLists)
            {
                foreach (AttributeSyntax attributeSyntax in attributeListSyntax.Attributes)
                {
                    IMethodSymbol attributeSymbol = context.SemanticModel.GetSymbolInfo(attributeSyntax).Symbol as IMethodSymbol;
                    if (attributeSymbol == null)
                    {
                        continue;
                    }

                    INamedTypeSymbol attributeContainingTypeSymbol = attributeSymbol.ContainingType;
                    if (attributeContainingTypeSymbol.ContainingNamespace.GetFullNamespace().StartsWith("KafkaExchanger.Attributes"))
                    {
                        return classDeclarationSyntax;
                    }
                }
            }

            return null;
        }

        //private static int _counter;
        public void Execute(Compilation compilation, ImmutableArray<ClassDeclarationSyntax> types, SourceProductionContext context)
        {
            //System.Diagnostics.Debugger.Launch();
            if (types.IsDefaultOrEmpty)
            {
                return;
            }

            if (string.IsNullOrEmpty(compilation.AssemblyName))
            {
                throw new System.NotSupportedException("Assembly don`t have name");
            }

//            context.AddSource($"perf.cs", $@"//
//// Counter: {Interlocked.Increment(ref _counter)}
//");

            var headerGenerator = new HeaderGenerator();
            headerGenerator.Generate(compilation.AssemblyName, context);

            var distinctTypes = types.GroupBy(gr => gr.Identifier.ValueText);
            var processor = new Processor();
            foreach (var item in distinctTypes)
            {
                var firstOfPartial = item.First();
                processor.ProcessAttributes(firstOfPartial, compilation);
            }

            processor.Generate(compilation.AssemblyName, context);
        }
    }
}