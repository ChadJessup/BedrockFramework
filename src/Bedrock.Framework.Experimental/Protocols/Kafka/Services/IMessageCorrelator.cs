#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests;
using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Responses;
using System;
using System.Threading.Tasks;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Services
{
    public interface IMessageCorrelator : IDisposable
    {
        bool HasCorrelationId(in short correlationId);
        short GetCorrelationId(in KafkaRequest request);
        ValueTask<KafkaResponse> GetCorrelationTask(in short correlationId);

        bool TryAdd(in short correlationId, in KafkaRequest kafkaRequest);
        bool TryCompleteCorrelation(in short correlationId, KafkaResponse response);

        KafkaResponse CreateEmptyCorrelatedResponse(in KafkaRequest request);
        KafkaResponse CreateEmptyCorrelatedResponse(in short correlationId);
    }
}
