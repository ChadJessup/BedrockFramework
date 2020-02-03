﻿#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Primitives;
using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public static class PayloadWriterExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PayloadWriter WriteString(this PayloadWriter writer, string? value)
        {
            var length = value?.Length ?? -1;
            writer.Write((short)length);

            if (value != null)
            {
                var bytes = Encoding.UTF8.GetBytes(value);
                writer.WriteBytes(ref bytes!);
            }

            return writer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PayloadWriter WriteBytes(this PayloadWriter writer, ref byte[]? bytes)
        {
            var length = bytes?.Length ?? -1;

            return writer.Write(bytes, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PayloadWriter WriteBytes(this PayloadWriter writer, ref ReadOnlySpan<byte> bytes)
        {
            writer.CurrentWriter.Write(bytes);

            return writer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PayloadWriter Write(this PayloadWriter writer, byte[]? bytes, int? length)
        {
            writer.Write(length ?? -1);

            if (bytes != null && length.HasValue)
            {
                var readOnlyBytes = new ReadOnlySpan<byte>(bytes, 0, length.Value);
                writer.WriteBytes(ref readOnlyBytes);
            }

            return writer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PayloadWriter WriteArray<T>(this PayloadWriter writer, T[]? array, Func<T, PayloadWriterContext, PayloadWriterContext> action)
        {
            writer.Write(array?.Length ?? -1);

            for (int i = 0; i < array?.Length; i++)
            {
                var modifiedContext = action(array[i], writer.Context);
                writer.Context = modifiedContext;
            }

            return writer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PayloadWriter WriteNullableString(this PayloadWriter writer, ref NullableString value)
        {
            writer.Write(value.Length);

            if (value.Length != -1)
            {
                writer.Write(value.Bytes.Span);
            }

            return writer;
        }

        internal static PayloadWriter StartCrc32Calculation(ref this PayloadWriter writer)
        {
            return writer;
        }

        internal static PayloadWriter EndCrc32Calculation(ref this PayloadWriter writer)
        {
            return writer;
        }
    }
}
