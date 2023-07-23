using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ProducerInfo
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.Data.ProducerData;

            builder.Append($@"
        public class ProducerInfo
        {{
            private ProducerInfo() {{ }}

            public ProducerInfo(
                string topicName
                )
            {{
                TopicName = topicName;
            }}

            public string TopicName {{ get; init; }}
        }}
");
        }
    }
}