#nullable enable
#pragma warning disable CA1815 // Override equals and operator equals on value types

using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public ref struct PayloadReader
    {
        public PayloadReaderContext Context;

        public PayloadReader(ref PayloadReaderContext context)
        {
            this.Context = context;
        }

        /// <summary>
        /// Creates a root instance of the <see cref="PayloadReader"/> struct.
        /// </summary>
        /// <param name="shouldWriteBigEndian">Whether or not to write bytes as big endian. Defaults to true.</param>
        public PayloadReader(in ReadOnlySequence<byte> input, bool shouldReadBigEndian)
        {
            this.Context = new PayloadReaderContext(input, shouldReadBigEndian);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out short value)
        {
            var span = this.Context.ReadOnlySequence
                .Slice(this.Context.BytesRead, sizeof(short))
                .FirstSpan;

            value = this.Context.ShouldReadBigEndian
                ? BinaryPrimitives.ReadInt16BigEndian(span)
                : BinaryPrimitives.ReadInt16LittleEndian(span);

            this.Context.Advance(sizeof(short));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out byte value)
        {
            var span = this.Context.ReadOnlySequence
                .Slice(this.Context.BytesRead, sizeof(byte))
                .FirstSpan;

            value = span[0];

            this.Context.Advance(sizeof(byte));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out long value)
        {
            var span = this.Context.ReadOnlySequence
                .Slice(this.Context.BytesRead, sizeof(long))
                .FirstSpan;

            value = this.Context.ShouldReadBigEndian
                ? BinaryPrimitives.ReadInt64BigEndian(span)
                : BinaryPrimitives.ReadInt64LittleEndian(span);

            this.Context.Advance(sizeof(long));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out int value)
        {
            var span = this.Context.ReadOnlySequence
                .Slice(this.Context.BytesRead, sizeof(long))
                .FirstSpan;

            value = this.Context.ShouldReadBigEndian
                ? BinaryPrimitives.ReadInt32BigEndian(span)
                : BinaryPrimitives.ReadInt32LittleEndian(span);

            this.Context.Advance(sizeof(int));

            return this;
        }
    }
}

#pragma warning restore CA1815 // Override equals and operator equals on value types
