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
        public class {TypeName()}
        {{
            public bool {Succsess()};
            public {TopicResponse.TypeFullName(requestAwaiter)} {Response()};
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "TryAddAwaiterResult";
        }

        public static string Succsess()
        {
            return "Succsess";
        }

        public static string Response()
        {
            return "Response";
        }
    }
}
