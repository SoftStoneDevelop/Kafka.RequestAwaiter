using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ConsumerInfo
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class ConsumerInfo
        {{
            private ConsumerInfo() {{ }}

            public ConsumerInfo(
                string topicName,
                string[] canAnswerService,
                int[] partitions
                )
            {{
                CanAnswerService = canAnswerService;
                TopicName = topicName;
                Partitions = partitions;
            }}

            public string TopicName {{ get; init; }}

            public string[] CanAnswerService {{ get; init; }}

            public int[] Partitions {{ get; init; }}
        }}
");
        }
    }
}