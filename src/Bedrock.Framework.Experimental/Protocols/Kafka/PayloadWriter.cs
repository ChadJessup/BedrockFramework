#nullable enable
#pragma warning disable CA1815 // Override equals and operator equals on value types

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public ref struct PayloadWriter
    {
        public readonly PipeWriter Writer;

        public PayloadWriterContext Context;

        /// <summary>
        /// Initializes a new child instance of the <see cref="PayloadWriter"/>,
        /// from the context of another <see cref="PayloadWriter"/>.
        /// </summary>
        /// <param name="settings">The context of the parent writer.</param>
        public PayloadWriter(ref PayloadWriterContext settings)
        {
            this.Context = settings;

            this.Writer = this.Context.Pipe.Writer;
        }

        /// <summary>
        /// Creates a root instance of the <see cref="PayloadWriter"/> struct.
        /// </summary>
        /// <param name="shouldWriteBigEndian">Whether or not to write bytes as big endian. Defaults to true.</param>
        public PayloadWriter(bool shouldWriteBigEndian)
        {
            var pipe = new Pipe();

            this.Context = new PayloadWriterContext(
                shouldWriteBigEndian,
                pipe);

            this.Writer = this.Context.Pipe.Writer;
        }

        /// <summary>
        /// Sets the location in a payload where a size will be calculated.
        /// Calculated size is between <see cref="StartCalculatingSize(string)"/>
        /// and a call to <see cref="EndSizeCalculation(string)"/> with the same name.
        /// </summary>
        /// <param name="name">The distinct name of a size calculation.</param>
        /// <returns>The <see cref="PayloadWriter"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter StartCalculatingSize(string name)
        {
            if (this.Context.SizeCalculations.ContainsKey(name))
            {
                throw new ArgumentException($"Unable to add another size calculation called: {name}", nameof(name));
            }

            var memory = this.Writer
                .GetMemory(sizeof(int))
                .Slice(0, sizeof(int));

            // Size calculation is not inclusive of the size value itself, + sizeof(int) starts
            // the calculation _after_ where the size value would be.
            this.Context.SizeCalculations[name] = (this.Context.BytesWritten + sizeof(int), memory);

            this.Context.Advance(sizeof(int));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter EndSizeCalculation(string name)
        {
            if (!this.Context.SizeCalculations.TryGetValue(name, out var calculation))
            {
                throw new ArgumentException($"Size calculation for {name} not found", nameof(name));
            }

            this.Context.SizeCalculations.Remove(name);

            var currentPosition = this.Context.BytesWritten;
            var size = (int)(currentPosition - calculation.position);

            var span = calculation.memory.Span;

            if (this.Context.ShouldWriteBigEndian)
            {
                BinaryPrimitives.WriteInt32BigEndian(span, size);
            }
            else
            {
                BinaryPrimitives.WriteInt32LittleEndian(span, size);
            }

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter Write(ReadOnlySpan<byte> bytes)
        {
            this.Writer.Write(bytes);
            this.Context.Advance(bytes.Length);

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter Write(Action<PayloadWriterContext> action)
        {
            action(this.Context);

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter Write(short value)
        {
            var span = this.Writer
                .GetSpan(sizeof(short))
                .Slice(0, sizeof(short));

            if (this.Context.ShouldWriteBigEndian)
            {
                BinaryPrimitives.WriteInt16BigEndian(span, value);
            }
            else
            {
                BinaryPrimitives.WriteInt16LittleEndian(span, value);
            }

            this.Context.Advance(sizeof(short));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter Write(byte value)
        {
            var span = this.Writer
                .GetSpan(sizeof(byte))
                .Slice(0, sizeof(byte));

            span[0] = value;

            this.Context.Advance(sizeof(byte));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter Write(long value)
        {
            var span = this.Writer
                .GetSpan(sizeof(long))
                .Slice(0, sizeof(long));

            if (this.Context.ShouldWriteBigEndian)
            {
                BinaryPrimitives.WriteInt64BigEndian(span, value);
            }
            else
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, value);
            }

            this.Context.Advance(sizeof(long));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadWriter Write(int value)
        {
            var span = this.Writer
                .GetSpan(sizeof(int))
                .Slice(0, sizeof(int));

            if (this.Context.ShouldWriteBigEndian)
            {
                BinaryPrimitives.WriteInt32BigEndian(span, value);
            }
            else
            {
                BinaryPrimitives.WriteInt32LittleEndian(span, value);
            }

            this.Context.Advance(sizeof(int));

            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryWritePayload(out ReadOnlySequence<byte> payload)
        {
            if (this.Context.SizeCalculations.Any())
            {
                throw new InvalidOperationException($"Not all size calculations have been closed. Call {nameof(PayloadWriter.EndSizeCalculation)} for: {string.Join(',', this.Context.SizeCalculations.Keys)}");
            }

            this.Writer.Complete();

            return this.WriteOutput(out payload);
        }

        private bool WriteOutput(out ReadOnlySequence<byte> payload)
        {
            while (this.Context.Pipe.Reader.TryRead(out var result))
            {
                if (!result.IsCompleted)
                {
                    continue;
                }

                if (result.IsCanceled)
                {

                }

                var output = new byte[this.Context.BytesWritten];
                var scopedSpan = result.Buffer.Slice(0, this.Context.BytesWritten);
                scopedSpan.CopyTo(output);

                payload = new ReadOnlySequence<byte>(output);

                this.Context.Pipe.Reader.Complete();

                return true;
            }

            payload = ReadOnlySequence<byte>.Empty;

            return false;
        }
    }
}

#pragma warning restore CA1815 // Override equals and operator equals on value types
