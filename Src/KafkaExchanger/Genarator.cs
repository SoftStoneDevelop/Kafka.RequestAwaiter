using KafkaExchanger.Generators;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Collections.Immutable;
using System.Linq;

namespace KafkaExchanger
{
    [Generator]
    public class Generator : IIncrementalGenerator
    {
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            //System.Diagnostics.Debugger.Launch();
            var classDeclarations = context.SyntaxProvider
                .CreateSyntaxProvider(
                predicate: (s, _) => IsSyntaxTargetForGeneration(s),
                transform: (ctx, _) => GetSemanticClass(ctx))
                .Where(m => m != null);

            var compilationAndClasses = context.CompilationProvider.Combine(classDeclarations.Collect());

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

        public void Execute(Compilation compilation, ImmutableArray<ClassDeclarationSyntax> types, SourceProductionContext context)
        {
            //System.Diagnostics.Debugger.Launch();
            if (types.IsDefaultOrEmpty)
            {
                return;
            }

            if(string.IsNullOrEmpty(compilation.AssemblyName))
            {
                throw new System.NotSupportedException("Assembly don`t have name");
            }

            var headerGenerator = new HeaderGenerator();
            headerGenerator.Generate(compilation.AssemblyName, context);

            var distinctTypes = types.Distinct().GroupBy(gr => gr.Identifier.ValueText);
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