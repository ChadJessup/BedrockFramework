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
                .ReadArray<KafkaApiKey>(out var apiKeys, this.ParseApiKey)
                .Complete();

            this.SupportedApis = apiKeys;
        }

        private ref PayloadReaderContext ParseApiKey(out KafkaApiKey kafkaApiKey, ref PayloadReaderContext context)
        {
            context.CreatePayloadReader()
                .Read(out short apiKey)
                .Read(out short min)
                .Read(out short max);

            kafkaApiKey = new KafkaApiKey((KafkaApiKeys)apiKey, min, max);

            return ref context;
        }
    }
}
