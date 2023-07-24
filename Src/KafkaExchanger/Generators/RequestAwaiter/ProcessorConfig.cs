using KafkaExchanger.AttributeDatas;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ProcessorConfig
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.GenerateData requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.Data.ProducerData;

            builder.Append($@"
        public class ProcessorConfig
        {{
            private ProcessorConfig() {{ }}

            public ProcessorConfig(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ConsumerInfo income{i}
");
            }

            if (requestAwaiter.IncomeDatas.Count > 0)
            {
                builder.Append(',');
            }
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ProducerInfo outcome{i}
");
            }

            builder.Append($@"
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState" : "")}
                {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.IncomeDatas)} afterCommit" : "")}
                {(producerData.CustomOutcomeHeader ? $@",{producerData.CustomOutcomeHeaderFunc(assemblyName)} createOutcomeHeader" : "")}
                {(producerData.CustomHeaders ? $@",{producerData.CustomHeadersFunc()} setHeaders" : "")},
                int buckets,
                int maxInFly
                )
            {{
                Buckets = buckets;
                MaxInFly = maxInFly;

                {(consumerData.CheckCurrentState ? "GetCurrentState = getCurrentState;" : "")}
                {(consumerData.UseAfterCommit ? "AfterCommit = afterCommit;" : "")}
                {(producerData.CustomOutcomeHeader ? @"CreateOutcomeHeader = createOutcomeHeader;" : "")}
                {(producerData.CustomHeaders ? @"SetHeaders = setHeaders;" : "")}
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                Income{i} = income{i};
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@"
                Outcome{i} = outcome{i};
");
            }

            builder.Append($@"
            }}

            public int Buckets {{ get; init; }}
            
            public int MaxInFly {{ get; init; }}

            {(consumerData.CheckCurrentState ? $"public {consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} GetCurrentState {{ get; init; }}" : "")}
            {(consumerData.UseAfterCommit ? $"public {consumerData.AfterCommitFunc(requestAwaiter.IncomeDatas)} AfterCommit {{ get; init; }}" : "")}
            {(producerData.CustomOutcomeHeader ? $"public {producerData.CustomOutcomeHeaderFunc(assemblyName)} CreateOutcomeHeader {{ get; init; }}" : "")}
            {(producerData.CustomHeaders ? $"public {producerData.CustomHeadersFunc()} SetHeaders {{ get; init; }}" : "")}
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
            public ConsumerInfo Income{i} {{ get; init; }}
");
            }

            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                builder.Append($@"
            public ProducerInfo Outcome{i} {{ get; init; }}
");
            }

            builder.Append($@"
        }}
");
        }
    }
}