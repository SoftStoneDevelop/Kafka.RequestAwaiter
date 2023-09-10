using GrcpService;
using Grpc.Core;
using System.Threading.Tasks;

namespace Responder0Console.Service
{
    public class RespondAPI : GrcpService.RespondService.RespondServiceBase
    {
        public override Task<HelloResponse> Hello(HelloRequest request, ServerCallContext context)
        {
            return Task.FromResult(new HelloResponse() { Text = $"0: Answer {request.Text}" });
        }
    }
}