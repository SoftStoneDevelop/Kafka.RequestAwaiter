using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Enums
{
    [Flags]
    internal enum OrderMatters
    {
        NotMatters = 0,
        ForProcess = 2,
        ForResponse = 4,
    }
}