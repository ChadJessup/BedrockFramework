using Bedrock.Framework.Experimental.Protocols.Kafka.Models;
using System.Buffers;
using System.Diagnostics;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Messages.Responses
{
    public class FetchResponseV11 : KafkaResponse
    {
        public int ThrottleTimeMs { get; private set; }
        public int SessionId { get; private set; }

        public override void FillResponse(in ReadOnlySequence<byte> response)
        {
            var reader = new SequenceReader<byte>(response);
            this.ThrottleTimeMs = reader.ReadInt32BigEndian();
            var errorCode = reader.ReadErrorCode();
            Debug.Assert(errorCode == KafkaErrorCode.NONE);

            this.SessionId = reader.ReadInt32BigEndian();
            this.ParseResponses(ref reader);
        }

        private void ParseResponses(ref SequenceReader<byte> reader)
        {
            int numberOfTopics = reader.ReadInt32BigEndian();

            for (int i = 0; i < numberOfTopics; i++)
            {
                var topic = reader.ReadString();

                int numberOfResponses = reader.ReadInt32BigEndian();

                for (int j = 0; j < numberOfResponses; j++)
                {
                    var partitionResponse = new PartitionResponse(ref reader);
                }
            }
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
