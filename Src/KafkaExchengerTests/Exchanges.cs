﻿using Confluent.Kafka;
using KafkaExchanger.Attributes;

namespace KafkaExchengerTests
{
    [RequestAwaiter(useLogger: false),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiterOneToOneSimple
    {

    }

    [RequestAwaiter(useLogger: false),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiterManyToOneSimple
    {

    }

    [RequestAwaiter(useLogger: true, commitAfter: 50, checkCurrentState: true, useAfterCommit: true, customOutcomeHeader: true, customHeaders: true),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class Huhuhu
    {

    }

    [Responder(useLogger: false),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class ResponderOneToOneSimple
    {

    }

    [RequestAwaiter(useLogger: false),
        Income(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Outcome(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class RequestAwaiterOneToOneProtobuff
    {

    }

    [Responder(useLogger: false),
        Income(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Outcome(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class ResponderOneToOneProtobuff
    {

    }
}