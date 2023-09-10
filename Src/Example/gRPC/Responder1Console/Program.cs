using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Responder1Console.Service;
using System;
using System.Threading.Tasks;

namespace Responder1Console
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            builder.WebHost.ConfigureKestrel((options) =>
            {
                options.ListenAnyIP(4401, o => o.Protocols = HttpProtocols.Http2);
            });
            builder.WebHost.UseKestrel();
            builder.Services
                .AddGrpc((options) =>
                {
                    options.MaxReceiveMessageSize = Int32.MaxValue;
                    options.MaxSendMessageSize = Int32.MaxValue;
                })
                ;

            var app = builder.Build();
            Configure(app);
            await app.RunAsync();
        }

        public static void Configure(IApplicationBuilder app)
        {
            app.UseRouting();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcService<RespondAPI>();
            });
        }
    }
}