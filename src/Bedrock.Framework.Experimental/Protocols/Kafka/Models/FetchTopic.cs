using System;
using System.Linq;
using System.Text;

namespace Bedrock.Framework.Experimental.Protocols.Kafka.Models
{
    public readonly struct FetchTopic
    {
        public readonly string Topic;
        public readonly FetchTopicPartition[] Partitions { get; }

        public FetchTopic(string topic, FetchTopicPartition[] partitions)
        {
            this.Topic = topic;
            this.Partitions = partitions;
        }

        private const int constantPayloadSize =
            sizeof(short) // topic name length
            + sizeof(int); // size of partition array

        public int GetSize()
        {
            return constantPayloadSize
                + Encoding.UTF8.GetByteCount(this.Topic)
                + this.Partitions.Sum(p => p.GetSize());
        }

        public override bool Equals(object obj)
        {
            if (obj == null || !(obj is FetchTopic))
            {
                return false;
            }

            var that = (FetchTopic)obj;

            return this.Topic.Equals(that.Topic)
                && this.Partitions.Equals(that.Partitions);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.Topic,
                this.Partitions);
        }

        public static bool operator ==(FetchTopic left, FetchTopic right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(FetchTopic left, FetchTopic right)
        {
            return !(left == right);
        }
    }
}
