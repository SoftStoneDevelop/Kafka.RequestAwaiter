using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class OutcomeMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.GenerateData requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeDatas = requestAwaiter.OutcomeDatas[i];
                builder.Append($@"
        public class Outcome{i}Message
        {{
");
                if (!outcomeDatas.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            public {outcomeDatas.KeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
                }

                builder.Append($@"
            public {outcomeDatas.ValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
            }
        }
    }
}