#nullable enable

using System;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Models
{
    public readonly struct MetadataPartitionV8
    {
        public readonly KafkaErrorCode ErrorCode;
        public readonly int PartitionIndex;
        public readonly int LeaderId;
        public readonly int LeaderEpoch;
        public readonly int[] ReplicaNodes;
        public readonly int[] IsrNodes;
        public readonly int[] OfflineReplicas;

        public MetadataPartitionV8(
            KafkaErrorCode errorCode,
            int partitionIndex,
            int leaderId,
            int leaderEpoch,
            int[] replicaNodes,
            int[] isrNodes,
            int[] offlineReplicas)
        {
            this.ErrorCode = errorCode;
            this.PartitionIndex = partitionIndex;
            this.LeaderId = leaderId;
            this.LeaderEpoch = leaderEpoch;
            this.ReplicaNodes = replicaNodes;
            this.IsrNodes = isrNodes;
            this.OfflineReplicas = offlineReplicas;
        }

        public override bool Equals(object? obj)
        {
            if (obj == null || !(obj is MetadataPartitionV8))
            {
                return false;
            }

            return this.GetHashCode().Equals(((MetadataPartitionV8)obj).GetHashCode());
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.ErrorCode,
                this.PartitionIndex,
                this.ReplicaNodes,
                this.LeaderId,
                this.IsrNodes);
        }

        public static bool operator ==(MetadataPartitionV8 left, MetadataPartitionV8 right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(MetadataPartitionV8 left, MetadataPartitionV8 right)
        {
            return !(left == right);
        }
    }
}
