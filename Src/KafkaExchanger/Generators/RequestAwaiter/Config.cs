using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Config
    {
        public static void Append(StringBuilder builder)
        {
            builder.Append($@"
        public class Config
        {{
            public Config(
                string groupId,
                string bootstrapServers,
                string outcomeTopicName,
                Consumer[] consumers
                )
            {{
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                OutcomeTopicName = outcomeTopicName;
                Consumers = consumers;
            }}

            public string GroupId {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public string OutcomeTopicName {{ get; init; }}

            public Consumer[] Consumers {{ get; init; }}
        }}
");
        }
    }
}