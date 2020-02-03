#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests;
using Bedrock.Framework.Infrastructure;
using Bedrock.Framework.Protocols;
using Microsoft.Extensions.Logging;
using System.Buffers;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public class KafkaMessageWriter : IMessageWriter<KafkaRequest>
    {
        private readonly ILogger<KafkaMessageWriter> logger;

        public KafkaMessageWriter(
            ILogger<KafkaMessageWriter> logger)
        {
            this.logger = logger;
        }

        public void WriteMessage(KafkaRequest message, IBufferWriter<byte> output)
        {
            var writer = new BufferWriter<IBufferWriter<byte>>(output);
            var clientId = message.ClientId;

            // On every outgoing message...
            var pw = new StrategyPayloadWriter<BigEndianStrategy>()
                .StartCalculatingSize("payloadSize")
                    .Write((short)message.ApiKey)
                    .Write(message.ApiVersion)
                    .Write(message.CorrelationId)
                    .WriteNullableString(ref clientId);

            // Since each Kafka Request/Response is versioned, have those objects
            // write/read on the PayloadWriter itself.
            message.WriteRequest(ref pw);

            pw.EndSizeCalculation("payloadSize");

            if (!pw.TryWritePayload(out var payload))
            {
                this.logger.LogError("Unable to retrieve payload for {KafkaRequest}", message);
            }

            if (message is MetadataRequestV0)
            {
                var lastSpan = payload.Slice(payload.Length - sizeof(int), sizeof(int)).ToArray();
                if (lastSpan[0] != 0 || lastSpan[1] != 0 || lastSpan[2] != 0 || lastSpan[3] != 0)
                {

                }
            }

            writer.Write(payload.ToSpan());

            writer.Commit();
        }
    }
}
