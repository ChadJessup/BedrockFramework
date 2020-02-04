#nullable enable
#pragma warning disable CA1815 // Override equals and operator equals on value types

using System.Buffers;
using System.Runtime.CompilerServices;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public class PayloadReaderContext
    {
        public readonly ReadOnlySequence<byte> ReadOnlySequence;
        public readonly bool ShouldReadBigEndian;
        public long BytesRead;

        public PayloadReaderContext(in ReadOnlySequence<byte> input, bool shouldReadBigEndian)
        {
            this.ShouldReadBigEndian = shouldReadBigEndian;
            this.ReadOnlySequence = input;
            this.BytesRead = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader CreatePayloadReader()
        {
            var context = this;

            return new PayloadReader(ref context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(int count)
        {
            this.BytesRead += count;
        }
    }
}

#pragma warning restore CA1815 // Override equals and operator equals on value types
