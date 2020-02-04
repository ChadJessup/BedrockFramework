using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using Bedrock.Framework.Infrastructure;
using System;
using System.Buffers;
using System.Linq;
using System.Text;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Requests
{
    public class FetchRequestV11 : KafkaRequest
    {
        public FetchRequestV11()
            : base(apiKey: KafkaApiKeys.Fetch, apiVersion: 11)
        {
            this.RackId = string.Empty;
        }

        public int ReplicaId { get; set; }
        public int MaxWaitTime { get; set; }
        public int MinBytes { get; set; }
        public int MaxBytes { get; set; }
        public byte IsolationLevel { get; set; }
        public int SessionId { get; set; }
        public int SessionEpoch { get; set; }
        public FetchTopic[] Topics { get; set; } = Array.Empty<FetchTopic>();
        public FetchForgottenTopic[] ForgottenTopics { get; set; } = Array.Empty<FetchForgottenTopic>();
        public string RackId { get; set; }

        private const int constantPayloadSize =
            sizeof(int) // replica_id
            + sizeof(int) // wait
            + sizeof(int) // min bytes
            + sizeof(int) // max bytes
            + sizeof(byte) // isolation_level
            + sizeof(int) // session_id
            + sizeof(int) // session epoch
            + sizeof(int) // topic array size value (0 for now)
            + sizeof(int) // topic array size value (0 for now)
            + sizeof(short); // length of string below

        //public override int GetPayloadSize()
        //{
            //return constantPayloadSize
                //+ this.Topics.Sum(t => t.GetSize())
                //+ Encoding.UTF8.GetByteCount(this.RackId);
        //}

        public void WriteRequest(ref BufferWriter<IBufferWriter<byte>> writer)
        {
            writer.WriteInt32BigEndian(this.ReplicaId);
            writer.WriteInt32BigEndian(this.MaxWaitTime);
            writer.WriteInt32BigEndian(this.MinBytes);
            writer.WriteInt32BigEndian(this.MaxBytes);
            writer.WriteByte(this.IsolationLevel);
            writer.WriteInt32BigEndian(this.SessionId);
            writer.WriteInt32BigEndian(this.SessionEpoch);
            this.WriteTopics(ref writer, this.Topics);
            writer.WriteArrayPreamble(0); // todo: forgotten topics data
            writer.WriteString(this.RackId);

            // TODO: chadj - 1/8/20 - Ignore for now, prototyping something out below...will move to feature branch in followup cl
            //var mw = new PayloadWriter(output, shouldCalculateSizeBeforeWriting: true, isBigEndian: true)
            //    .WriteCalculatedSize()
            //    .Write(this.ReplicaId)
            //    .Write(this.MaxWaitTime)
            //    .Write(this.MinBytes)
            //    .Write(this.MaxBytes);

            //if (mw.TryWritePayload(out var payload))
            //{
            //    Debug.Assert(payload.Length == 20);
            //}
            //else
            //{

            //}
        }

        private void WriteTopics(ref BufferWriter<IBufferWriter<byte>> writer, in FetchTopic[] topics)
        {
            writer.WriteArrayPreamble(topics.Length);
            for (int i = 0; i < topics.Length; i++)
            {
                var topic = topics[i];
                writer.WriteString(topic.Topic);
                this.WriteTopicPartitions(ref writer, topic.Partitions);
            }
        }

        private void WriteTopicPartitions(
            ref BufferWriter<IBufferWriter<byte>> writer,
            FetchTopicPartition[] partitions)
        {
            writer.WriteArrayPreamble(partitions.Length);

            for (int i = 0; i < partitions.Length; i++)
            {
                var partition = partitions[i];

                writer.WriteInt32BigEndian(partition.Partition);
                writer.WriteInt32BigEndian(partition.CurrentLeaderEpoch);
                writer.WriteInt64BigEndian(partition.FetchOffset);
                writer.WriteInt64BigEndian(partition.LogStartOffset);
                writer.WriteInt32BigEndian(partition.PartitionMaxBytes);
            }
        }

        public override void WriteRequest<TStrategy>(ref StrategyPayloadWriter<TStrategy> writer)
        {
            throw new NotImplementedException();
        }
    }
}

// https://kafka.apache.org/protocol#The_Messages_Fetch
/*
Fetch Request (Version: 11) => replica_id max_wait_time min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id 
  replica_id => INT32
  max_wait_time => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic [partitions] 
    topic => STRING
    partitions => partition current_leader_epoch fetch_offset log_start_offset partition_max_bytes 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic [partitions] 
    topic => STRING
    partitions => INT32
  rack_id => STRING
*/
