﻿using Confluent.Kafka;
using KafkaExchanger.Attributes;
using System;
using System.Threading.Tasks;

namespace Responder0Console
{
    [Responder(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class ResponderOneToOneSimple
    {

    }

    internal class Program
    {
        static async Task Main(string[] args)
        {
            var bootstrapServers = "localhost:9194, localhost:9294, localhost:9394";
            var inputName = "RAOutputSimple";
            var responderName = "RAResponder2";

            var responder2 = new ResponderOneToOneSimple();
            var responder1Config =
                new ResponderOneToOneSimple.Config(
                groupId: responderName,
                serviceName: responderName,
                bootstrapServers: bootstrapServers,
                new ResponderOneToOneSimple.ProcessorConfig[]
                {
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: (input, s) =>
                        {
                            Console.WriteLine($"Input: {input.Input0Message.Value}");
                            var result = new ResponderOneToOneSimple.OutputMessage()
                            {
                                Output0Message = new ResponderOneToOneSimple.Output0Message()
                                {
                                    Value = $"2: Answer {input.Input0Message.Value}"
                                }
                            };

                            return Task.FromResult(result);
                        },
                        loadCurrentHorizon: static async (input0partitions) => { return await Task.FromResult(0L); },
                        input0: new ResponderOneToOneSimple.ConsumerInfo(inputName, new int[] { 0 })
                        ),
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: (input, s) =>
                        {
                            Console.WriteLine($"Input: {input.Input0Message.Value}");
                            var result = new ResponderOneToOneSimple.OutputMessage()
                            {
                                Output0Message = new ResponderOneToOneSimple.Output0Message()
                                {
                                    Value = $"2: Answer {input.Input0Message.Value}"
                                }
                            };

                            return Task.FromResult(result);
                        },
                        loadCurrentHorizon: static async (input0partitions) => { return await Task.FromResult(0L); },
                        input0: new ResponderOneToOneSimple.ConsumerInfo(inputName, new int[] { 1 })
                        ),
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: (input, s) =>
                        {
                            Console.WriteLine($"Input: {input.Input0Message.Value}");
                            var result = new ResponderOneToOneSimple.OutputMessage()
                            {
                                Output0Message = new ResponderOneToOneSimple.Output0Message()
                                {
                                    Value = $"2: Answer {input.Input0Message.Value}"
                                }
                            };

                            return Task.FromResult(result);
                        },
                        loadCurrentHorizon: static async (input0partitions) => { return await Task.FromResult(0L); },
                        input0: new ResponderOneToOneSimple.ConsumerInfo(inputName, new int[] { 2 })
                        )
                }
                );

            var pool = new KafkaExchanger.Common.ProducerPoolNullString(
                5,
                bootstrapServers,
                static (config) =>
                {
                    config.LingerMs = 2;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            Console.WriteLine("Start Responder");
            await responder2.Start(config: responder1Config, output0Pool: pool);
            Console.WriteLine("Responder started");

            while (true)
            {
                var read = Console.ReadLine();
                if (read == "exit")
                {
                    break;
                }
            }

            await responder2.StopAsync();
        }
    }
}