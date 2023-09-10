using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RequestAwaiterConsole
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var unary = new gRPCUnary();
            await unary.Run();
        }
    }
}