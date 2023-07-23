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

            if(requestAwaiter.Data is RequestAwaiterData)
            {
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
            }
            
            builder.Append($@"
                {(requestAwaiter.Data is ResponderData ? $",{consumerData.CreateResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} createResponse" : "")}
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState" : "")}
                {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.IncomeDatas)} afterCommit" : "")}
                {(producerData.AfterSendResponse ? $@",{producerData.AfterSendResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} afterSendResponse" : "")}
                {(producerData.CustomOutcomeHeader ? $@",{producerData.CustomOutcomeHeaderFunc(assemblyName)} createOutcomeHeader" : "")}
                {(producerData.CustomHeaders ? $@",{producerData.CustomHeadersFunc()} setHeaders" : "")},
                int buckets,
                int maxInFly
                )
            {{
                Buckets = buckets;
                MaxInFly = maxInFly;

                {(requestAwaiter.Data is ResponderData ? $"CreateResponse = createResponse;" : "")}
                {(consumerData.CheckCurrentState ? "GetCurrentState = getCurrentState;" : "")}
                {(consumerData.UseAfterCommit ? "AfterCommit = afterCommit;" : "")}
                {(producerData.AfterSendResponse ? @"AfterSendResponse = afterSendResponse;" : "")}
                {(producerData.CustomOutcomeHeader ? @"CreateOutcomeHeader = createOutcomeHeader;" : "")}
                {(producerData.CustomHeaders ? @"SetHeaders = setHeaders;" : "")}
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                Income{i} = income{i};
");
            }

            if (requestAwaiter.Data is RequestAwaiterData)
            {
                for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
                {
                    builder.Append($@"
                Outcome{i} = outcome{i};
");
                }
            }
            
            builder.Append($@"
            }}

            public int Buckets {{ get; init; }}
            
            public int MaxInFly {{ get; init; }}

            {(requestAwaiter.Data is ResponderData ? $"public {consumerData.CreateResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} CreateResponse {{ get; init; }}" : "")}
            {(consumerData.CheckCurrentState ? $"public {consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} GetCurrentState {{ get; init; }}" : "")}
            {(consumerData.UseAfterCommit ? $"public {consumerData.AfterCommitFunc(requestAwaiter.IncomeDatas)} AfterCommit {{ get; init; }}" : "")}
            {(producerData.AfterSendResponse ? $"public {producerData.AfterSendResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} AfterSendResponse {{ get; init; }}" : "")}
            {(producerData.CustomOutcomeHeader ? $"public {producerData.CustomOutcomeHeaderFunc(assemblyName)} CreateOutcomeHeader {{ get; init; }}" : "")}
            {(producerData.CustomHeaders ? $"public {producerData.CustomHeadersFunc()} SetHeaders {{ get; init; }}" : "")}
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
            public ConsumerInfo Income{i} {{ get; init; }}
");
            }

            if (requestAwaiter.Data is RequestAwaiterData)
            {
                for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
                {
                    builder.Append($@"
            public ProducerInfo Outcome{i} {{ get; init; }}
");
                }
            }

            builder.Append($@"
        }}
");
        }
    }
}