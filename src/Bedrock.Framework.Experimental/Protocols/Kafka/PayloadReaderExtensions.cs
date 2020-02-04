#nullable enable
#pragma warning disable CA1815 // Override equals and operator equals on value types

using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using System;
using System.Runtime.CompilerServices;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public static class PayloadReaderExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref PayloadReader ReadAndThrowOnError(this ref PayloadReader reader, out KafkaErrorCode errorCode)
        {
            reader.Read(out short error);

            errorCode = (KafkaErrorCode)error;
            if (errorCode != KafkaErrorCode.NONE)
            {
                throw new InvalidOperationException($"Error Code Received: {errorCode}");
            }

            return ref reader;
        }

        public delegate ref PayloadReaderContext ParseElement<TElement, PayloadReaderContext>(out TElement element, ref PayloadReaderContext context);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref PayloadReader ReadArray<TElement>(
            this ref PayloadReader reader,
            out TElement[] elements,
            ParseElement<TElement, PayloadReaderContext> action)
        {
            reader.Read(out int arraySize);

            if (arraySize == -1)
            {
                elements = Array.Empty<TElement>();

                return ref reader;
            }

            elements = new TElement[arraySize];

            for (int i = 0; i < arraySize; i++)
            {
                var modifiedContext = action(out TElement element, ref reader.Context);
                elements[i] = element;

                reader.Context = modifiedContext;
            }

            return ref reader;
        }
    }
}

#pragma warning restore CA1815 // Override equals and operator equals on value types
