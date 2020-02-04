#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using Bedrock.Framework.Infrastructure;
using System;
using System.Buffers;
using System.Linq;
using System.Text;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests
{
    public class MetadataRequestV8 : KafkaRequest
    {
        public MetadataRequestV8()
            : base(KafkaApiKeys.Metadata, apiVersion: 8)
        {
        }

        public string[]? Topics { get; set; } = Array.Empty<string>();
        public bool AllowAutoTopicCreation { get; set; }
        public bool IncludeClusterAuthorizedOperations { get; set; }
        public bool IncludeTopicAuthorizedOperations { get; set; }

        private const int constantPayloadSize =
            sizeof(byte) // allow topic creation bool
            + sizeof(byte) // include clust auth bool
            + sizeof(byte) // include topic auth
            + sizeof(int); // topic array length

        //public override int GetPayloadSize()
        //{
        //    // Todo: speed this up, we'll traverse each string twice.
        //    return constantPayloadSize
        //        + (sizeof(short) * (this.Topics?.Length ?? 0)) //  each short saying how long each string is
        //        + (this.Topics?.Sum(t => Encoding.UTF8.GetByteCount(t)) ?? 0);
        //}

        public void WriteRequest(ref BufferWriter<IBufferWriter<byte>> writer)
        {
            if (this.Topics == null)
            {
                writer.WriteArrayPreamble(null);
            }
            else
            {
                var topicCount = this.Topics.Length;
                writer.WriteArrayPreamble(topicCount);

                for (int i = 0; i < topicCount; i++)
                {
                    var topic = this.Topics[i];
                    writer.WriteString(topic);
                }
            }

            writer.WriteBoolean(this.AllowAutoTopicCreation);
            writer.WriteBoolean(this.IncludeClusterAuthorizedOperations);
            writer.WriteBoolean(this.IncludeTopicAuthorizedOperations);
        }

        public override void WriteRequest<TStrategy>(ref StrategyPayloadWriter<TStrategy> writer)
        {
            throw new NotImplementedException();
        }
    }
}
