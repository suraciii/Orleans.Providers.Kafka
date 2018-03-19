using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaOptions
    {
        public string BrokerList { get; set; }
        public StreamProviderDirection Direction { get; set; } = StreamProviderDirection.ReadWrite;
    }

    public static class KafkaConfigurationExtensions
    {

        public static Dictionary<string, object> ToKafkaProducerConfig(this KafkaOptions kafkaOptions)
        {
            return new Dictionary<string, object>
            {
                { "bootstrap.servers", kafkaOptions.BrokerList },
            };
        }

        public static Dictionary<string, object> ToKafkaConsumerConfig(this KafkaReceiverOptions receiverOptions, KafkaOptions kafkaOptions)
        {
            return new Dictionary<string, object>
            {
                { "group.id", receiverOptions.ConsumerGroup },
                { "enable.auto.commit", true },
                { "auto.commit.interval.ms", 5000 },
                { "statistics.interval.ms", 60000 },
                { "bootstrap.servers", kafkaOptions.BrokerList },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };
        }

    }

    public class KafkaReceiverOptions
    {
        public List<string> TopicList { get; set; }
        public string ConsumerGroup { get; set; }
        public int TotalQueueCount { get; set; }
    }

    public class KafkaStreamCachePressureOptions
    {

        public int CacheSize { get; set; }

        /// <summary>
        /// SlowConsumingPressureMonitorConfig
        /// </summary>
        public double? SlowConsumingMonitorFlowControlThreshold { get; set; }

        /// <summary>
        /// SlowConsumingMonitorPressureWindowSize
        /// </summary>
        public TimeSpan? SlowConsumingMonitorPressureWindowSize { get; set; }

        /// <summary>
        /// AveragingCachePressureMonitorFlowControlThreshold, AveragingCachePressureMonitor is turn on by default. 
        /// User can turn it off by setting this value to null
        /// </summary>
        public double? AveragingCachePressureMonitorFlowControlThreshold { get; set; } = DEFAULT_AVERAGING_CACHE_PRESSURE_MONITORING_THRESHOLD;
        public const double AVERAGING_CACHE_PRESSURE_MONITORING_OFF = 1.0;
        public const double DEFAULT_AVERAGING_CACHE_PRESSURE_MONITORING_THRESHOLD = 1.0 / 3.0;
    }
}
