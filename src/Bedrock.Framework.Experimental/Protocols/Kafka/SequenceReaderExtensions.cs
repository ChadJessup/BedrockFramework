#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using System;
using System.Buffers;
using System.Text;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public static class SequenceReaderExtensions
    {
        public static string? ReadNullableString(this ref SequenceReader<byte> reader)
        {
            var length = reader.ReadInt16BigEndian();

            if (length == -1)
            {
                return null;
            }

            reader.Rewind(sizeof(short));

            return ReadString(ref reader);
        }

        public static string ReadString(this ref SequenceReader<byte> reader)
        {
            var length = reader.ReadInt16BigEndian();

            if (length == 0)
            {
                return "";
            }

            // This should exist: https://github.com/dotnet/corefx/issues/26104
            // if(Utf8Parser.TryParse(reader.CurrentSpan.Slice(0, length), out string value))
            var value = Encoding.UTF8.GetString(reader.UnreadSpan.Slice(0, length));

            reader.Advance(length);

            return value;
        }

        public static KafkaErrorCode ReadErrorCode(this ref SequenceReader<byte> reader)
        {
            var errorCode = reader.ReadInt16BigEndian();

            return (KafkaErrorCode)errorCode;
        }

        public static short ReadInt16BigEndian(this ref SequenceReader<byte> reader)
        {
            reader.TryReadBigEndian(out short value);

            return value;
        }

        public static short ReadInt16LittleEndian(this ref SequenceReader<byte> reader)
        {
            reader.TryReadLittleEndian(out short value);

            return value;
        }

        public static bool ReadBool(this ref SequenceReader<byte> reader)
        {
            var value = reader.ReadByte();

            // anything other than 0 is true
            return value == 0
                ? false
                : true;
        }

        public static byte ReadByte(this ref SequenceReader<byte> reader)
        {
            reader.TryRead(out byte value);

            return value;
        }

        public static byte[]? ReadBytes(this ref SequenceReader<byte> reader)
        {
            var size = reader.ReadInt32BigEndian();

            if (size == -1)
            {
                return null;
            }

            if (size == 0)
            {
                return Array.Empty<byte>();
            }

            var bytes = new byte[size];
            reader.Sequence.Slice(reader.Consumed, size).CopyTo(bytes);
            reader.Advance(size);

            return bytes;
        }

        public static int ReadArrayCount(this ref SequenceReader<byte> reader)
            => ReadInt32BigEndian(ref reader);

        public static int ReadInt32BigEndian(this ref SequenceReader<byte> reader)
        {
            reader.TryReadBigEndian(out int value);

            return value;
        }

        public static int ReadInt32LittleEndian(this ref SequenceReader<byte> reader)
        {
            reader.TryReadLittleEndian(out int value);

            return value;
        }

        public static long ReadInt64BigEndian(this ref SequenceReader<byte> reader)
        {
            reader.TryReadBigEndian(out long value);

            return value;
        }

        public static long ReadInt64LittleEndian(this ref SequenceReader<byte> reader)
        {
            reader.TryReadLittleEndian(out long value);

            return value;
        }

    }
}
