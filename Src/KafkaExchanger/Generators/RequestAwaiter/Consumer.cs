using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Consumer
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class Consumer
        {{
            private Consumer() {{ }}

            public Consumer(
                string groupName
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@",
                ConsumerInfo income{i}
");
            }
            builder.Append($@",
                int buckets,
                int maxInFly
                )
            {{
                Buckets = buckets;
                MaxInFly = maxInFly;
                GroupName = groupName;
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                Income{i} = income{i};
");
            }
            builder.Append($@"
            }}
            
            /// <summary>
            /// To identify the logger
            /// </summary>
            public string GroupName {{ get; init; }}
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
            public ConsumerInfo Income{i} {{ get; init; }}
");
            }
            builder.Append($@"
            public int Buckets {{ get; init; }}
            
            public int MaxInFly {{ get; init; }}
        }}
");
        }
    }
}