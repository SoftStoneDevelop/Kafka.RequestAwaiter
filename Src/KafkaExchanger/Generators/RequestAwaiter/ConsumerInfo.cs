using KafkaExchanger.AttributeDatas;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ConsumerInfo
    {
        public static void Append(
            StringBuilder builder
            )
        {
            builder.Append($@"
        public class ConsumerInfo
        {{
            private ConsumerInfo() {{ }}

            public ConsumerInfo(
                string topicName,
                int[] partitions
                )
            {{
                TopicName = topicName;
                Partitions = partitions;
            }}

            public string TopicName {{ get; init; }}

            public int[] Partitions {{ get; init; }}
        }}
");
        }
    }
}