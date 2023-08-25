using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class BucketNames
    {
        public static string BucketId()
        {
            return "BucketId";
        }

        public static string PBucketId()
        {
            return BucketId().ToCamel().ToPrivate();
        }

        public static string OutputTopicName(OutputData outputData)
        {
            return $"{outputData.NameCamelCase}TopicName";
        }

        public static string POutputTopicName(OutputData outputData)
        {
            return OutputTopicName(outputData).ToPrivate();
        }

        public static string PTCSPartitionsName(InputData inputData)
        {
            return $"_tcsPartitions{inputData.NamePascalCase}";
        }

        public static string PConsumeCanceledName(InputData inputData)
        {
            return $"_consume{inputData.NamePascalCase}Canceled";
        }

        public static string PMaxInFly()
        {
            return ProcessorConfig.MaxInFlyNameCamel().ToPrivate();
        }

        public static string PCurrentStateFunc()
        {
            return ProcessorConfig.CurrentStateFuncNameCamel().ToPrivate();
        }

        public static string PAfterCommitFunc()
        {
            return ProcessorConfig.AfterCommitFuncNameCamel().ToPrivate();
        }

        public static string PAfterSendFunc(OutputData outputData)
        {
            return ProcessorConfig.AfterSendFuncNameCamel(outputData).ToPrivate();
        }

        public static string PLoadOutputFunc(OutputData outputData)
        {
            return ProcessorConfig.LoadOutputFuncNameCamel(outputData).ToPrivate();
        }

        public static string PCheckOutputStatusFunc(OutputData outputData)
        {
            return ProcessorConfig.CheckOutputStatusFuncNameCamel(outputData).ToPrivate();
        }

        public static string Partitions(InputData inputData)
        {
            return $"{inputData.NamePascalCase}Partitions";
        }
    }
}