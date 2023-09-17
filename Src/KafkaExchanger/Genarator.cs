using KafkaExchanger.Generators;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Xml.Linq;

namespace KafkaExchanger
{
    [Generator]
    public class Generator : IIncrementalGenerator
    {
        public class ByArrayComparer : IEqualityComparer<(ImmutableArray<ClassDeclarationSyntax> Nodes, Compilation compilation)>
        {
            public bool Equals(
               (ImmutableArray<ClassDeclarationSyntax> Nodes, Compilation compilation) left,
               (ImmutableArray<ClassDeclarationSyntax> Nodes, Compilation compilation) rigth
                )
            {
                if (left.Nodes.Length != rigth.Nodes.Length)
                {
                    return false;
                }

                for (int i = 0; i < left.Nodes.Length; i++)
                {
                    var leftNode = left.Nodes[i];
                    var rigthNode = rigth.Nodes[i];

                    if (!leftNode.IsEquivalentTo(rigthNode))
                    {
                        return false;
                    }
                }

                return true;
            }

            public int GetHashCode((ImmutableArray<ClassDeclarationSyntax> Nodes, Compilation compilation) obj)
            {
                int hash = 0;
                unchecked
                {
                    for (int i = 0; i < obj.Nodes.Length; i++)
                    {
                        hash += obj.Nodes[i].GetHashCode();
                    }
                }

                return hash;
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
                .Select((sel, _) => sel.Distinct())
                .SelectMany(
                (sel, _) =>
                sel.GroupBy(gr => gr.Identifier.ValueText)
                .Select(grSel => grSel.ToImmutableArray())
                )
                ;

            var compilationAndClasses =
                classDeclarations.Combine(context.CompilationProvider)
                .WithComparer(new ByArrayComparer())
                ;

            context.RegisterSourceOutput(compilationAndClasses,
                (spc, source) => Execute(source.Item2, source.Item1, spc));
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
        public void Execute(Compilation compilation, ImmutableArray<ClassDeclarationSyntax> partialGroup, SourceProductionContext context)
        {
            //System.Diagnostics.Debugger.Launch();
            if (partialGroup.IsDefaultOrEmpty)
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

            var processor = new Processor();
            foreach (var type in partialGroup)
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                processor.ProcessAttributes(type, compilation, context.CancellationToken);
            }

            processor.Generate(compilation.AssemblyName, context, context.CancellationToken);
        }
    }
}