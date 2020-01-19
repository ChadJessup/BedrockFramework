using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using Bedrock.Framework.Infrastructure;
using System.Buffers;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests
{
    public class ApiVersionsRequestV2 : KafkaRequest
    {
        public ApiVersionsRequestV2()
            : base(KafkaApiKeys.ApiVersions, apiVersion: 2)
        {
        }

        public override void WriteRequest(ref BufferWriter<IBufferWriter<byte>> output)
        {
        }
    }
}
