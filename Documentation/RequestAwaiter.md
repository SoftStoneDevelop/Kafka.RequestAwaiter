Constructors:

```C#

public RequestAwaiterAttribute(
  bool useLogger = true,
  uint commitAfter = 1u,
  bool checkCurrentState = false,
  bool useAfterCommit = false,
  bool customOutcomeHeader = false,
  bool customHeaders = false
)

```

Types can be any type or generated from Protobuff types(The main thing is that it should be inherited from IMessage<T>)

Usage:

```C#

[RequestAwaiter(useLogger: false),
        Income(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Income(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Outcome(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
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
                            income0: new RequestAwaiter.ConsumerInfo(
                                topicName: "input0Name",
                                canAnswerService: new [] { "responderName0" },
                                partitions: new int[] { 0 }
                                ),
                            income1: new RequestAwaiter.ConsumerInfo(
                                topicName: "input1Name",
                                canAnswerService: new [] { "responderName1" },
                                partitions: new int[] { 0 }
                                ),
                            outcome0: new RequestAwaiter.ProducerInfo("outputName"),
                            buckets: 2,
                            maxInFly: 100
                            ),
                        new RequestAwaiter.ProcessorConfig(
                            income0: new RequestAwaiter.ConsumerInfo(
                                topicName: "input0Name",
                                canAnswerService: new [] { "responderName0" },
                                partitions: new int[] { 1 }
                                ),
                            income1: new RequestAwaiter.ConsumerInfo(
                                topicName: "input1Name",
                                canAnswerService: new [] { responderName1 },
                                partitions: new int[] { 1 }
                                ),
                            outcome0:new RequestAwaiter.ProducerInfo("outputName"),
                            buckets: 2,
                            maxInFly: 100
                            )
                    }
                    );
  reqAwaiter.Start(
                reqAwaiterConfitg, 
                producerPool0: pool
                );
  
  using var answer = await reqAwaiter.Produce(
    new protobuff.SimpleKey() { Id = 12  },
    new protobuff.SimpleValue() { Id = 12, Message = "Hello" }
    );
    
    //process answer.Result
```

![Request awaiter shema](https://github.com/SoftStoneDevelop/KafkaExchanger/blob/main/Documentation/request_awaiter.svg)
Each bucket have you own consumer thread for aech input topic. When you Produce request then RequestAwaiter chose bucket not busy bicket and send it from him.
`maxInFly` it is limit of maximum requests in moment in the bucket. After requests(equal to `maxInFly`) completed we commit offsets topics.
