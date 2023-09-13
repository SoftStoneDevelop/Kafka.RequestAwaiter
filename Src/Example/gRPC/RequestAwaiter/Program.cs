using System.Threading.Tasks;

namespace RequestAwaiterConsole
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            //var unary = new gRPCUnary();
            //await unary.Run();

            var stream = new gRPCStream();
            await stream.Run();
        }
    }
}