using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class OutputMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            AppendOutputMessages(builder, assemblyName, responder);
        }

        private static void AppendOutputMessages(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            for (int i = 0; i < responder.OutputDatas.Count; i++)
            {
                var outputData = responder.OutputDatas[i];
                builder.Append($@"
        public class {outputData.MessageTypeName}
        {{
");
                if (!outputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            public {outputData.KeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
                }

                builder.Append($@"
            public {outputData.ValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
            }
        }
    }
}