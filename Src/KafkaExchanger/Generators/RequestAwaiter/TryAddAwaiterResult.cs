using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class TryAddAwaiterResult
    {
        public static void Append(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class TryAddAwaiterResult
        {{
            public bool Succsess;
            public {requestAwaiter.TypeSymbol.Name}.TopicResponse Response;
        }}
");
        }
    }
}
