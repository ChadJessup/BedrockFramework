using System;
using System.Buffers;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Models
{
    public readonly struct PartitionResponse
    {
        public readonly PartitionHeader Header;
        public readonly RecordSetHeader RecordSetHeader;

        public PartitionResponse(ref SequenceReader<byte> reader)
        {
            this.Header = new PartitionHeader(ref reader);
            this.RecordSetHeader = new RecordSetHeader(ref reader);
        }

        public override bool Equals(object obj)
        {
            if(obj == null || !(obj is PartitionResponse))
            {
                return false;
            }

            var that = (PartitionResponse)obj;

            return this.Header.Equals(that.Header)
                && this.RecordSetHeader.Equals(that.RecordSetHeader);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.Header,
                this.RecordSetHeader);
        }

        public static bool operator ==(PartitionResponse left, PartitionResponse right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(PartitionResponse left, PartitionResponse right)
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
