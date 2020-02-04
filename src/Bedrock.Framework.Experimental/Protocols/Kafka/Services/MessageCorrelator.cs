#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests;
using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Responses;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Services
{
    public class MessageCorrelator : IMessageCorrelator, IValueTaskSource<KafkaResponse>
    {
        // TODO: Move to some form of reflection at startup
        private readonly Dictionary<Type, Type> requestResponseCorrelations
            = new Dictionary<Type, Type>
            {
                // Returns the supported Api calls and versions for associated version.
                { typeof(ApiVersionsRequestV0), typeof(ApiVersionsResponseV0) },

                // Returns broker and/or topic metadata
                { typeof(MetadataRequestV0),    typeof(MetadataResponseV0) },

                // Fetches data from Topics and partitions
                { typeof(FetchRequestV0),       typeof(FetchResponseV0) },

                // Writes data to a Topic+Partition
                {typeof(ProduceRequestV0),      typeof(ProduceResponseV0) },
            };

        private short correlationId = 1;

        private readonly ConcurrentDictionary<short, KafkaRequest> correlations
            = new ConcurrentDictionary<short, KafkaRequest>();

        private readonly IServiceProvider services;
        private readonly ILogger<MessageCorrelator> logger;
        private readonly ManualResetValueTaskSourceCore<KafkaResponse> vts;

        public MessageCorrelator(ILogger<MessageCorrelator> logger, IServiceProvider serviceProvider)
        {
            this.services = serviceProvider;
            this.logger = logger;
        }

        public bool HasCorrelationId(in short correlationId)
            => this.correlations.ContainsKey(correlationId);

        public bool TryAdd(in short correlationId, in KafkaRequest kafkaRequest)
        {
            if (this.correlations.TryAdd(correlationId, kafkaRequest))
            {
                this.logger.LogTrace("Added {CorrelationId} for {KafkaRequest}", correlationId, kafkaRequest.GetType().FullName);

                return true;
            }
            else
            {
                this.logger.LogTrace("Failed to add {CorrelationId} for {KafkaRequest}", correlationId, kafkaRequest.GetType().FullName);

                return false;
            }
        }

        public ValueTask<KafkaResponse> GetCorrelationTask(in short correlationId)
        {
            if (!this.correlations.TryGetValue(correlationId, out var data))
            {
                throw new ArgumentException($"Unknown correlationId: {correlationId}");
            }

            return new ValueTask<KafkaResponse>(this, correlationId);
        }

        public bool TryCompleteCorrelation(in short correlationId, KafkaResponse response)
        {
            if (!this.correlations.ContainsKey(correlationId))
            {
                throw new ArgumentException($"Unknown correlationId: {correlationId}");
            }

            if (response is null)
            {
                throw new ArgumentNullException(nameof(response));
            }

            if (this.correlations.TryRemove(correlationId, out var correlations))
            {
                this.vts.SetResult(response);

                return true;
            }

            return false;
        }

        public KafkaResponse CreateEmptyCorrelatedResponse(in short correlationId)
        {
            if (!this.correlations.ContainsKey(correlationId))
            {
                throw new ArgumentException($"Unexpected correlationId: {correlationId}", nameof(correlationId));
            }

            return this.CreateEmptyCorrelatedResponse(this.correlations[correlationId]);
        }

        public KafkaResponse CreateEmptyCorrelatedResponse(in KafkaRequest request)
        {
            var requestType = request.GetType();
            if (!this.requestResponseCorrelations.ContainsKey(requestType))
            {
                throw new ArgumentException($"Unknown {nameof(KafkaRequest)} type: {requestType.FullName}", nameof(request));
            }

            var responseType = this.requestResponseCorrelations[requestType];

            var response = (KafkaResponse)ActivatorUtilities.CreateInstance(this.services, responseType);

            this.logger.LogDebug("Activated {KafkaResponse} from {KafkaRequest}", response, request);

            return response;
        }

        public short GetCorrelationId(in KafkaRequest request)
        {
            if (!this.TryAdd(this.correlationId, request))
            {
                throw new InvalidOperationException($"Non-unique correlationId provided in {request.GetType().Name}");
            }

            return this.correlationId;
        }

        public KafkaResponse GetResult(short token)
        {
            return default;
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            return ValueTaskSourceStatus.Pending;
        }

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
