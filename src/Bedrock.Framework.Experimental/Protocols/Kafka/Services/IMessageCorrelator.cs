#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests;
using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Responses;
using System;
using System.Threading.Tasks;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Services
{
    public interface IMessageCorrelator : IDisposable
    {
        bool HasCorrelationId(in int correlationId);
        int GetCorrelationId(in KafkaRequest request);
        Task<KafkaResponse> GetCorrelationTask(in int correlationId);

        bool TryAdd(in int correlationId, in KafkaRequest kafkaRequest);
        bool TryCompleteCorrelation(in int correlationId, KafkaResponse response);

        KafkaResponse CreateEmptyCorrelatedResponse(in KafkaRequest request);
        KafkaResponse CreateEmptyCorrelatedResponse(in int correlationId);
    }
}
