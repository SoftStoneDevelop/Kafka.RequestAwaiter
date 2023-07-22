using Microsoft.Extensions.Configuration;
using NUnit.Framework;
using System.IO;

namespace KafkaExchengerTests
{
    [SetUpFixture]
    public class GlobalSetUp
    {
        public static IConfiguration Configuration;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false);
            Configuration = builder.Build();
        }
    }
}