using GrcpService;
using Grpc.Core;
using System.Threading.Tasks;

namespace Responder1Console.Service
{
    public class RespondAPI : GrcpService.RespondService.RespondServiceBase
    {
        public override Task<HelloResponse> Hello(HelloRequest request, ServerCallContext context)
        {
            return Task.FromResult(new HelloResponse() { Text = $"1: Answer {request.Text}" });
        }
    }
}