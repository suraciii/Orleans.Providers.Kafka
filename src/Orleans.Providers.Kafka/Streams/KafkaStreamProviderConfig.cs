using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaStreamProviderConfig
    {


        public KafkaStreamProviderConfig(IProviderConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            var enumerableConnectionStrings = connectionStrings as IList<Uri> ?? connectionStrings.ToList();
            if (connectionStrings == null || !enumerableConnectionStrings.Any()) throw new ArgumentNullException(nameof(connectionStrings));

            _connectionStrings = enumerableConnectionStrings;
            _topicName = topicName;
            _consumerGroupName = consumerGroupName;

            NumOfQueues = DefaultNumOfQueues;
            CacheSize = DefaultCacheSize;
            ProduceBatchSize = DefaultProduceBatchSize;
            TimeToWaitForBatchInMs = DefaultTimeToWaitForBatchInMs;
            MaxBytesInMessageSet = DefaultMaxBytesInMessageSet;
            AckLevel = DefaultAckLevel;
            ReceiveWaitTimeInMs = DefaultReceiveWaitTimeInMs;
            OffsetCommitInterval = DefaultOffsetCommitInterval;
            ShouldInitWithLastOffset = DefaultShouldInitWithLastOffset;
            MaxMessageSizeInBytes = DefaultMaxMessageSizeInBytes;
            CacheTimespanInSeconds = DefaultCacheTimespanInSeconds;
            CacheNumOfBuckets = DefaultCacheNumOfBucketsParam;
            MetricsPort = DefaultMetricsPort;
            IncludeMetrics = DefaultIncludeMetrics;
            UsingExternalMetrics = DefaultUsingExternalMetrics;
            KafkaBatchFactory = DefaultKafkaBatchFactory;
        }
    }
}
