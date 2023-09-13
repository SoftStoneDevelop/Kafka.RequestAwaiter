using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.Pool
{
    internal static class ByName
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {

            builder.Append($@"
        private class {TypeName()} : {ProduceInfo.TypeFullName(assemblyName, outputData)}
        {{
            public string {Name()};
        }}
");
        }

        public static string TypeFullName(
            string assemblyName,
            OutputData outputData
            )
        {
            return $"{Pool.TypeFullName(assemblyName, outputData)}.{TypeName()}";
        }

        public static string TypeName()
        {
            return $"ByName";
        }

        public static string Name()
        {
            return $"Name";
        }
    }
}