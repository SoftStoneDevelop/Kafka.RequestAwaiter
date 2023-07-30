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
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ConsumerInfo input{i}
");
            }

            if (requestAwaiter.InputDatas.Count > 0)
            {
                builder.Append(',');
            }
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ProducerInfo output{i}
");
            }

            builder.Append($@"
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState" : "")}
                {(consumerData.UseAfterCommit ? $",{consumerData.AfterCommitFunc(requestAwaiter.InputDatas)} afterCommit" : "")}
                {(producerData.CustomOutputHeader ? $@",{producerData.CustomOutputHeaderFunc(assemblyName)} createOutputHeader" : "")}
                {(producerData.CustomHeaders ? $@",{producerData.CustomHeadersFunc()} setHeaders" : "")},
                int buckets,
                int maxInFly
                )
            {{
                Buckets = buckets;
                MaxInFly = maxInFly;

                {(consumerData.CheckCurrentState ? "GetCurrentState = getCurrentState;" : "")}
                {(consumerData.UseAfterCommit ? "AfterCommit = afterCommit;" : "")}
                {(producerData.CustomOutputHeader ? @"CreateOutputHeader = createOutputHeader;" : "")}
                {(producerData.CustomHeaders ? @"SetHeaders = setHeaders;" : "")}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
                Input{i} = input{i};
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@"
                Output{i} = output{i};
");
            }

            builder.Append($@"
            }}

            public int Buckets {{ get; init; }}
            
            public int MaxInFly {{ get; init; }}

            {(consumerData.CheckCurrentState ? $"public {consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} GetCurrentState {{ get; init; }}" : "")}
            {(consumerData.UseAfterCommit ? $"public {consumerData.AfterCommitFunc(requestAwaiter.InputDatas)} AfterCommit {{ get; init; }}" : "")}
            {(producerData.CustomOutputHeader ? $"public {producerData.CustomOutputHeaderFunc(assemblyName)} CreateOutputHeader {{ get; init; }}" : "")}
            {(producerData.CustomHeaders ? $"public {producerData.CustomHeadersFunc()} SetHeaders {{ get; init; }}" : "")}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
            public ConsumerInfo Input{i} {{ get; init; }}
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@"
            public ProducerInfo Output{i} {{ get; init; }}
");
            }

            builder.Append($@"
        }}
");
        }
    }
}