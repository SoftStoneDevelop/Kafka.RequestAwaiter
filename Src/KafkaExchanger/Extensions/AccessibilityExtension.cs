using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Extensions
{
    internal static class AccessibilityExtension
    {
        internal static string ToName(this Accessibility accessibility)
        {
            if(accessibility == Accessibility.Private)
            {
                return "private";
            }

            if (accessibility == Accessibility.Protected)
            {
                return "protected";
            }

            if (accessibility == Accessibility.Internal)
            {
                return "internal";
            }

            return "public";
        }
    }
}