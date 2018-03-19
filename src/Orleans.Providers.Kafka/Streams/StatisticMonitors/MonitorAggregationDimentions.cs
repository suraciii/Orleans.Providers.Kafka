using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Providers.Kafka.Streams.StatisticMonitors
{
    public class KafkaMonitorAggregationDimensions
    {

        public KafkaMonitorAggregationDimensions()
        { }

        public KafkaMonitorAggregationDimensions(KafkaMonitorAggregationDimensions dimensions)
        { }

    }

    public class KafkaReceiverMonitorDimensions : KafkaMonitorAggregationDimensions
    {

        public List<TopicPartition> TopicPartitions { get; set; }

        public KafkaReceiverMonitorDimensions(KafkaMonitorAggregationDimensions dimensions, List<TopicPartition> topicPartitions)
            : base(dimensions)
        {
            this.TopicPartitions = topicPartitions;
        }

        public KafkaReceiverMonitorDimensions()
        {
        }
    }

    public class KafkaCacheMonitorDimensions : KafkaReceiverMonitorDimensions
    {

        public string BlockPoolId { get; set; }

        public KafkaCacheMonitorDimensions(KafkaMonitorAggregationDimensions dimensions, List<TopicPartition> topicPartitions, string blockPoolId)
            : base(dimensions, topicPartitions)
        {
            this.BlockPoolId = blockPoolId;
        }

        public KafkaCacheMonitorDimensions()
        {
        }
    }

    public class KafkaBlockPoolMonitorDimensions : KafkaMonitorAggregationDimensions
    {

        public string BlockPoolId { get; set; }

        public KafkaBlockPoolMonitorDimensions(KafkaMonitorAggregationDimensions dimensions, string blockPoolId)
            : base(dimensions)
        {
            this.BlockPoolId = blockPoolId;
        }

        public KafkaBlockPoolMonitorDimensions()
        {
        }
    }
}
