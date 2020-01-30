#nullable enable
#pragma warning disable CA1815 // Override equals and operator equals on value types

using System.Buffers;
using System.Runtime.CompilerServices;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public ref struct PayloadReader
    {
        private SequenceReader<byte> sequence;
        private readonly PayloadReaderContext context;
        internal ReadOnlySequence<byte> ReadOnlySequence;

        public long BytesRead => this.sequence.Consumed;

        /// <summary>
        /// Creates a root instance of the <see cref="PayloadReader"/> struct.
        /// </summary>
        /// <param name="shouldWriteBigEndian">Whether or not to write bytes as big endian. Defaults to true.</param>
        public PayloadReader(in ReadOnlySequence<byte> input, bool shouldReadBigEndian)
        {
            this.context = new PayloadReaderContext(shouldReadBigEndian);
            this.ReadOnlySequence = input;
            this.sequence = new SequenceReader<byte>(this.ReadOnlySequence);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out short value)
        {
            value = this.context.ShouldReadBigEndian
                ? this.sequence.ReadInt16BigEndian()
                : this.sequence.ReadInt16LittleEndian();

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out byte value)
        {
            value = this.sequence.ReadByte();
            //this.sequence.Advance(sizeof(byte));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out long value)
        {
            value = this.context.ShouldReadBigEndian
                ? this.sequence.ReadInt64BigEndian()
                : this.sequence.ReadInt64LittleEndian();

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadReader Read(out int value)
        {
            value = this.context.ShouldReadBigEndian
                ? this.sequence.ReadInt32BigEndian()
                : this.sequence.ReadInt32LittleEndian();

            return this;
        }
    }
}

#pragma warning restore CA1815 // Override equals and operator equals on value types
