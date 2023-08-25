using KafkaExchanger.Extensions;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class Interface
    {
        public static void Append(
            string assemblyName,
            KafkaExchanger.Datas.Responder responder,
            StringBuilder builder
            )
        {
            StartInterface(responder, builder);
            Start(responder, builder);
            StopAsync(responder, builder);
            EndInterfaceOrClass(builder);
        }

        private static void StartInterface(
            KafkaExchanger.Datas.Responder responder,
            StringBuilder builder
            )
        {
            builder.Append($@"
    {responder.TypeSymbol.DeclaredAccessibility.ToName()} partial interface I{responder.TypeSymbol.Name}Responder
    {{
");
        }

        private static void Start(
            KafkaExchanger.Datas.Responder responder,
            StringBuilder builder
            )
        {
            builder.Append($@"
        public void Start(
            {Config.TypeFullName(responder)} config
");
            for (int i = 0; i < responder.OutputDatas.Count; i++)
            {
                var outputData = responder.OutputDatas[i];
                builder.Append($@",
            {outputData.FullPoolInterfaceName} {outputData.NameCamelCase}Pool
");
            }
            builder.Append($@"
            );
");
        }

        private static void StopAsync(
            KafkaExchanger.Datas.Responder responder,
            StringBuilder builder
            )
        {
            builder.Append($@"
        public Task StopAsync();
");
        }

        private static void EndInterfaceOrClass(StringBuilder builder)
        {
            builder.Append($@"
    }}
");
        }
    }
}