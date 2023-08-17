Constructors:

```C#

public RequestAwaiterAttribute(
  bool useLogger = true,
  bool checkCurrentState = false,
  bool useAfterCommit = false,
  bool afterSend = false,
  bool AddAwaiterCheckStatus = false
)

```

Types can be any type or generated from Protobuff types(The main thing is that it should be inherited from IMessage<T>)

Usage:

```C#

[RequestAwaiter(useLogger: false),
        Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue), new [] { "Responder0" }),//input0
        Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue), new [] { "Responder1" }),//input1
        Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))//output0
        ]
    public partial class RequestAwaiter
    {

    }

//Implement IProducerPoolProtoProto for producerPool self or use delault generated pool ProducerPoolProtoProto
var pool = new KafkaExchanger.Common.ProducerPoolProtoProto(
                20,
                bootstrapServers,
                static (config) =>
                {
                    config.LingerMs = 1;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );

var reqAwaiter = new RequestAwaiter();
var reqAwaiterConfitg =
                new RequestAwaiter.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: bootstrapServers,
                    processors: new RequestAwaiter.ProcessorConfig[]
                    {
                        new RequestAwaiter.ProcessorConfig(
                            input0: new RequestAwaiter.ConsumerInfo(
                                topicName: "input0Name",
                                partitions: new int[] { 0 }
                                ),
                            input1: new RequestAwaiter.ConsumerInfo(
                                topicName: "input1Name",
                                partitions: new int[] { 0 }
                                ),
                            output0: new RequestAwaiter.ProducerInfo("outputName"),
                            buckets: 2,
                            maxInFly: 100
                            ),
                        new RequestAwaiter.ProcessorConfig(
                            input0: new RequestAwaiter.ConsumerInfo(
                                topicName: "input0Name",
                                partitions: new int[] { 1 }
                                ),
                            input1: new RequestAwaiter.ConsumerInfo(
                                topicName: "input1Name",
                                partitions: new int[] { 1 }
                                ),
                            output0:new RequestAwaiter.ProducerInfo("outputName"),
                            buckets: 2,
                            maxInFly: 100
                            )
                    }
                    );
  reqAwaiter.Start(
                reqAwaiterConfitg, 
                producerPool0: pool
                );
  
  using var response = await reqAwaiter.Produce(
    new protobuff.SimpleKey() { Id = 12  },
    new protobuff.SimpleValue() { Id = 12, Message = "Hello" }
    );
    
    //process response
    //response.Input0Message
    //response.Input1Message
```

![Request awaiter shema](https://github.com/SoftStoneDevelop/KafkaExchanger/blob/main/Documentation/request_awaiter.svg)
Each bucket have you own consumer thread for aech input topic. When you Produce request then RequestAwaiter chose bucket not busy bicket and send it from him.
`maxInFly` it is limit of maximum requests in moment in the bucket. After requests(equal to `maxInFly`) completed we commit offsets topics.

We can also accept answers in one input topic from different services:
```C#

[RequestAwaiter(useLogger: false),
        Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue), new [] { "Responder0", "Responder1" }),
        Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class RequestAwaiter
    {

    }

    //process response
    //response.Input0Message0 - from Responder0
    //response.Input1Message1 - from Responder1
```
![Request awaiter shema](https://github.com/SoftStoneDevelop/KafkaExchanger/blob/main/Documentation/request_awaiter_one.svg)
