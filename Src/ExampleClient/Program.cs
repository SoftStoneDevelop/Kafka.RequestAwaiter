using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace ExampleClient
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole());

            await TopicCreators.CreateSimpleTopics();
            await TopicCreators.CreateProtobuffTopics();

            await SimpleExchangeRunner.RunExchange(loggerFactory);

            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine();
            }

            await ProtobuffExchangeRunner.RunExchange(loggerFactory);

            await SimpleListenerRunner.RunListener(loggerFactory);

            for (int i = 0; i < 10; i++)
            {
                Console.WriteLine();
            }

            await ProtobuffListenerRunner.RunListener(loggerFactory);
        }
    }
}