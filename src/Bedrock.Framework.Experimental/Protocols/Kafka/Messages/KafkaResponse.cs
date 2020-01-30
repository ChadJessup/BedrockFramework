#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using System;
using System.Buffers;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Responses
{
    public abstract class KafkaResponse
    {
        public abstract void FillResponse(in ReadOnlySequence<byte> payload);
    }
}
