using System;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Models
{
    public readonly struct FetchTopicPartition
    {
        public readonly int Partition;
        public readonly int CurrentLeaderEpoch;
        public readonly long FetchOffset;
        public readonly long LogStartOffset;
        public readonly int PartitionMaxBytes;

        public FetchTopicPartition(
            int partition,
            int currentLeaderEpoch,
            long fetchOffset,
            long logStartOffset,
            int partitionMaxBytes)
        {
            this.Partition = partition;
            this.CurrentLeaderEpoch = currentLeaderEpoch;
            this.FetchOffset = fetchOffset;
            this.LogStartOffset = logStartOffset;
            this.PartitionMaxBytes = partitionMaxBytes;
        }

        private const int constantPayloadSize =
            sizeof(int) // partition idx
            + sizeof(int) // current leader epoch
            + sizeof(long) // fetch offset
            + sizeof(long) // log start offset
            + sizeof(int); // partition max bytes

        public int GetSize()
        {
            return constantPayloadSize;
        }

        public override bool Equals(object obj)
        {
            if (obj == null || !(obj is FetchTopicPartition))
            {
                return false;
            }

            var that = (FetchTopicPartition)obj;

            return this.CurrentLeaderEpoch.Equals(that.CurrentLeaderEpoch)
                && this.FetchOffset.Equals(that.FetchOffset)
                && this.LogStartOffset.Equals(that.LogStartOffset)
                && this.Partition.Equals(that.Partition)
                && this.PartitionMaxBytes.Equals(that.PartitionMaxBytes);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.CurrentLeaderEpoch,
                this.FetchOffset,
                this.LogStartOffset,
                this.Partition,
                this.PartitionMaxBytes);
        }

        public static bool operator ==(FetchTopicPartition left, FetchTopicPartition right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(FetchTopicPartition left, FetchTopicPartition right)
        {
            return !(left == right);
        }
    }
}