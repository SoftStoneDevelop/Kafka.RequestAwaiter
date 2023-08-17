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
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;

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
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@",
                {requestAwaiter.Data.AfterSendFunc(assemblyName, outputData, i)} afterSendOutput{i}
");
                }

                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.Data.LoadOutputMessageFunc(assemblyName, outputData, i, requestAwaiter.InputDatas)} loadOutput{i}Message
");
                }
            }

            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@",
                {requestAwaiter.Data.AddAwaiterCheckStatusFunc(assemblyName, requestAwaiter.InputDatas)} addAwaiterCheckStatus
");
            }

            builder.Append($@",
                int buckets,
                int maxInFly
                )
            {{
                Buckets = buckets;
                MaxInFly = maxInFly;
");
            if(consumerData.CheckCurrentState)
            {
                builder.Append($@"
                GetCurrentState = getCurrentState;
");
            }

            if (consumerData.UseAfterCommit)
            {
                builder.Append($@"
                AfterCommit = afterCommit;
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@"
                AfterSendOutput{i} = afterSendOutput{i};
");
                }

                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                LoadOutput{i}Message = loadOutput{i}Message;
");
                }
            }

            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@"
                AddAwaiterCheckStatus = addAwaiterCheckStatus;
");
            }

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
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                builder.Append($@"
            public ConsumerInfo Input{i} {{ get; init; }}
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public ProducerInfo Output{i} {{ get; init; }}
");
                if(requestAwaiter.Data.AfterSend)
                {
                    builder.Append($@"
            public {requestAwaiter.Data.AfterSendFunc(assemblyName, outputData, i)} AfterSendOutput{i} {{ get; init; }}
");
                }

                if (requestAwaiter.Data.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                public {requestAwaiter.Data.LoadOutputMessageFunc(assemblyName, outputData, i, requestAwaiter.InputDatas)} LoadOutput{i}Message {{ get; init; }}
");
                }
            }

            if (requestAwaiter.Data.AddAwaiterCheckStatus)
            {
                builder.Append($@"
            public {requestAwaiter.Data.AddAwaiterCheckStatusFunc(assemblyName, requestAwaiter.InputDatas)} AddAwaiterCheckStatus {{ get; init; }}
");
            }

            builder.Append($@"
        }}
");
        }
    }
}