#nullable enable

using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests;
using Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Responses;
using Bedrock.Framework.Experimental.Protocols.Kafka.Primitives;
using Bedrock.Framework.Experimental.Protocols.Kafka.Services;
using Bedrock.Framework.Protocols;
using Microsoft.AspNetCore.Connections;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Bedrock.Framework.Experimental.Protocols.Kafka
{
    public class KafkaProtocol : IDisposable
    {
#pragma warning disable CA1034 // Nested types should not be visible
        public static class Keys
#pragma warning restore CA1034 // Nested types should not be visible
        {
            public const string ClientId = nameof(ClientId);

            // Precomputed ClientId as a Kafka Nullable string.
            public const string ClientIdNullable = nameof(ClientIdNullable);
            public const string ApiVersions = nameof(ApiVersions);
            public const string ReadTask = nameof(ReadTask);
            public const string Reader = nameof(Reader);
            public const string Writer = nameof(Writer);
        }

        private readonly ConcurrentDictionary<ConnectionContext, (ProtocolReader reader, ProtocolWriter writer)> readerWriters
            = new ConcurrentDictionary<ConnectionContext, (ProtocolReader reader, ProtocolWriter writer)>();

        private readonly IKafkaConnectionManager connectionManager;
        private readonly KafkaMessageReader messageReader;
        private readonly KafkaMessageWriter messageWriter;
        private readonly ILogger<KafkaProtocol> logger;
        private readonly IMessageCorrelator correlator;
        private readonly IServiceProvider services;

        public KafkaProtocol(
            IKafkaConnectionManager connectionManager,
            IServiceProvider serviceProvider,
            ILogger<KafkaProtocol> logger,
            IMessageCorrelator correlator,
            KafkaMessageReader reader,
            KafkaMessageWriter writer)
        {
            this.services = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

            this.connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            this.correlator = correlator ?? throw new ArgumentNullException(nameof(correlator));
            this.messageReader = reader ?? throw new ArgumentNullException(nameof(reader));
            this.messageWriter = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        public async ValueTask<KafkaResponse> SendAsync<TRequest>(ConnectionContext connection, TRequest request, CancellationToken token = default)
            where TRequest : KafkaRequest
        {
            var writer = (ProtocolWriter)connection.Items[KafkaProtocol.Keys.Writer];
            request.ClientId = (NullableString)connection.Items[KafkaProtocol.Keys.ClientIdNullable];
            request.CorrelationId = this.correlator.GetCorrelationId(request);

            await writer.WriteAsync(this.messageWriter, request, token).ConfigureAwait(false);

            return await this.correlator.GetCorrelationTask(request.CorrelationId);
        }

        public async Task ReceiveMessages(ConnectionContext connection, CancellationToken token = default)
        {
            var reader = (ProtocolReader)connection.Items[KafkaProtocol.Keys.Reader];

            while (!token.IsCancellationRequested)
            {
                var result = await reader.ReadAsync(this.messageReader, token).ConfigureAwait(false);
                var correlationId = result.Message.CorrelationId;

                if (result.IsCompleted)
                {
                    throw new ConnectionAbortedException();
                }

                if (result.Message is NullResponse)
                {
                    throw new InvalidOperationException($"Got back {nameof(NullResponse)}");
                }

                reader.Advance();
            }
        }

        public async ValueTask SetClientConnectionAsync(ConnectionContext connection, string clientId)
        {
            connection.Items.Add(KafkaProtocol.Keys.ClientId, clientId);
            connection.Items.Add(KafkaProtocol.Keys.ClientIdNullable, new NullableString(clientId));

            var reader = connection.CreateReader();
            var writer = connection.CreateWriter();

            // Cache the reader and writer for easy access...
            this.readerWriters.TryAdd(connection, (reader, writer));

            // And also associate them with the connection itself.
            connection.Items.Add(KafkaProtocol.Keys.Reader, reader);
            connection.Items.Add(KafkaProtocol.Keys.Writer, writer);

            await this.StartupConnectionAsync(connection, connection.ConnectionClosed).ConfigureAwait(false);
            this.connectionManager.TryAddConnection(connection);

            this.logger.LogInformation("{ConnectionId}: Added {Connection} to ConnectionManager", connection.ConnectionId, connection);
        }

        private async ValueTask StartupConnectionAsync(ConnectionContext connection, CancellationToken token = default)
        {
            var readTask = this.ReceiveMessages(connection, token);

            connection.Items.Add(KafkaProtocol.Keys.ReadTask, readTask);
            var count = 0;
            while (count++ < 100)
            {
                await Task.Delay(100);

                // Send the lowest ApiVersionRequest - this should work for any broker
                var lowestApiLevels = (ApiVersionsResponseV0)await this.SendAsync(
                    connection,
                    ApiVersionsRequestV0.AllSupportedApis,
                    token)
                    .ConfigureAwait(false);

                var metadataResponse = (MetadataResponseV0)await this.SendAsync(
                    connection,
                    MetadataRequestV0.AllTopics,
                    token)
                    .ConfigureAwait(false);

                Debug.Assert(lowestApiLevels.SupportedApis.Any());

                // Store allowed api messages and their versions on the connection itself.
                // connection.Items.Add(KafkaProtocol.Keys.ApiVersions, lowestApiLevels.SupportedApis);

                this.logger.LogInformation($"{connection.ConnectionId}: Retrieved ApiKeys from {connection.RemoteEndPoint}");
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    var sb = new StringBuilder();
                    foreach (var api in lowestApiLevels.SupportedApis)
                    {
                        sb.AppendLine($"{connection.ConnectionId}: {api.ApiKey} Min: {api.MinimumVersion} Max: {api.MaximumVersion}");
                    }

                    this.logger.LogDebug(sb.ToString());
                }

                Debug.Assert(metadataResponse.Brokers.Any());
                Debug.Assert(metadataResponse.Topics.Any());

                this.logger.LogInformation($"{connection.ConnectionId}: Retrieved Metadata from {connection.RemoteEndPoint}");
                if (this.logger.IsEnabled(LogLevel.Debug))
                {
                    var sb = new StringBuilder(capacity: metadataResponse.Brokers.Length + metadataResponse.Topics.Sum(t => t.Partitions.Length));
                    foreach (var broker in metadataResponse.Brokers)
                    {
                        sb.AppendLine($"{connection.ConnectionId}: Broker: {broker.Host}:{broker.Port}: NodeId: {broker.NodeId}");
                    }

                    var indent = new string(' ', 4);
                    foreach (var topic in metadataResponse.Topics)
                    {
                        sb.AppendLine($"{indent}{topic.Name}: Partitions: {topic.Partitions.Length}");
                    }

                    this.logger.LogDebug(sb.ToString());

                    this.logger.LogInformation($"Iteration: {count}");
                }
            }
            // TODO: get all brokers, and establish connections for them.
        }

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~KafkaProtocol()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
