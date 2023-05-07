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
            await SimpleExchangeRunner.RunExchange(loggerFactory);

            for (int i = 0; i < 5; i++)
            {
                Console.WriteLine();
            }

            await TopicCreators.CreateProtobuffTopics();
            await ProtobuffExchangeRunner.RunExchange(loggerFactory);
        }
    }
}