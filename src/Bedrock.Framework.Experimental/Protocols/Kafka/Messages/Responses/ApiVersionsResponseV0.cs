#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using System;
using System.Buffers;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Responses
{
    public class ApiVersionsResponseV0 : KafkaResponse
    {
        public KafkaApiKey[] SupportedApis { get; private set; } = Array.Empty<KafkaApiKey>();

        public override void FillResponse(in ReadOnlySequence<byte> response)
        {
            var reader = new PayloadReader(response, shouldReadBigEndian: true);

            reader.ReadAndThrowOnError(out var _)
                .ReadArray(out var apiKeys, this.ParseApiKey);

            this.SupportedApis = apiKeys;
        }

        private KafkaApiKey ParseApiKey(ReadOnlySequence<byte> sequence)
        {
            new PayloadReader(sequence, shouldReadBigEndian: true)
                .Read(out short apiKey)
                .Read(out short min)
                .Read(out short max);

            return new KafkaApiKey((KafkaApiKeys)apiKey, min, max);
        }
    }
}
