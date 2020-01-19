using System;
using System.Buffers;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Models
{
    public readonly struct PartitionHeader
    {
        public readonly int Partition;
        public readonly KafkaErrorCode ErrorCode;
        public readonly long HighWatermark;
        public readonly long LatestStableOffset;
        public readonly long LogStartOffset;
        public readonly AbortedTransaction[] AbortedTransactions;
        public readonly int PreferredReadReplica;

        public PartitionHeader(ref SequenceReader<byte> reader)
        {
            this.Partition = reader.ReadInt32BigEndian();
            this.ErrorCode = reader.ReadErrorCode();
            this.HighWatermark = reader.ReadInt64BigEndian();
            this.LatestStableOffset = reader.ReadInt64BigEndian();
            this.LogStartOffset = reader.ReadInt64BigEndian();
            this.AbortedTransactions = ParseAbortedTransactions(ref reader);
            this.PreferredReadReplica = reader.ReadInt32BigEndian();
        }

        private static AbortedTransaction[] ParseAbortedTransactions(ref SequenceReader<byte> reader)
        {
            var numberOfAbortedTransactions = reader.ReadArrayCount();

            if (numberOfAbortedTransactions == -1)
            {
                return Array.Empty<AbortedTransaction>();
            }

            var abortedTransactions = new AbortedTransaction[numberOfAbortedTransactions];

            for (int i = 0; i < numberOfAbortedTransactions; i++)
            {
                abortedTransactions[i] = new AbortedTransaction(ref reader);
            }

            return abortedTransactions;
        }

        public override bool Equals(object obj)
        {
            if (obj == null || !(obj is PartitionHeader))
            {
                return false;
            }

            var that = (PartitionHeader)obj;

            return this.GetHashCode().Equals(that.GetHashCode());
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.AbortedTransactions,
                this.ErrorCode,
                this.HighWatermark,
                this.LatestStableOffset,
                this.LogStartOffset,
                this.Partition,
                this.PreferredReadReplica);
        }

        public static bool operator ==(PartitionHeader left, PartitionHeader right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(PartitionHeader left, PartitionHeader right)
        {
            return !(left == right);
        }
    }
}

// https://kafka.apache.org/protocol#The_Messages_Fetch
/*
 Fetch Response (Version: 11) => throttle_time_ms error_code session_id [responses] 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic [partition_responses] 
    topic => STRING
    partition_responses => partition_header record_set 
      partition_header => partition error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica 
        partition => INT32
        error_code => INT16
        high_watermark => INT64
        last_stable_offset => INT64
        log_start_offset => INT64
        aborted_transactions => producer_id first_offset 
          producer_id => INT64
          first_offset => INT64
        preferred_read_replica => INT32
      record_set => RECORDS
*/
