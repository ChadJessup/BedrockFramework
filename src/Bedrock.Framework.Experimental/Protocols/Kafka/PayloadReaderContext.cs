#nullable enable
#pragma warning disable CA1815 // Override equals and operator equals on value types

using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public class PayloadReaderContext
    {
        public readonly bool ShouldReadBigEndian;

        public PayloadReaderContext(bool shouldReadBigEndian)
        {
            this.ShouldReadBigEndian= shouldReadBigEndian;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader CreatePayloadReader()
        {
            var context = this;

            return new PayloadReader(ref context);
        }
    }
}

#pragma warning restore CA1815 // Override equals and operator equals on value types
