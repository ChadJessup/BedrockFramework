#nullable enable

using System;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Models
{
    public readonly struct MetadataTopicV8
    {
        public readonly MetadataPartitionV8[] Partitions;
        public readonly KafkaErrorCode ErrorCode;
        public readonly int TopicAuthorization;
        public readonly bool IsInternalTopic;
        public readonly string Name;

        public MetadataTopicV8(
            KafkaErrorCode error,
            string name,
            bool isInternalTopic,
            MetadataPartitionV8[] partitions,
            int topicAuthorization)
        {
            this.Partitions = partitions;
            this.ErrorCode = error;
            this.IsInternalTopic = isInternalTopic;
            this.Name = name;
            this.TopicAuthorization = topicAuthorization;
        }

        public override bool Equals(object? obj)
        {
            if (obj == null || !(obj is MetadataTopicV0))
            {
                return false;
            }

            var that = (MetadataTopicV8)obj;

            return this.ErrorCode.Equals(that.ErrorCode)
                && this.Name.Equals(that.Name)
                && this.Partitions.Equals(that.Partitions);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.ErrorCode,
                this.Name,
                this.Partitions);
        }

        public static bool operator ==(MetadataTopicV8 left, MetadataTopicV8 right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(MetadataTopicV8 left, MetadataTopicV8 right)
        {
            return !(left == right);
        }
    }
}
