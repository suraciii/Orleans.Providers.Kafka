using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Streams
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
        public int TotalQueueCount { get; set; } = 2;
    }

    public class KafkaStreamCachePressureOptions
    {
        public int CacheSize { get; set; } = 20;
    }

    public class KafkaOptionsValidator : IConfigurationValidator
    {
        private readonly KafkaOptions options;
        private readonly string name;
        public KafkaOptionsValidator(KafkaOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }
        public void ValidateConfiguration()
        {
            if (String.IsNullOrEmpty(options.BrokerList))
                throw new OrleansConfigurationException($"{nameof(KafkaOptions)} on stream provider {this.name} is invalid. {nameof(KafkaOptions.BrokerList)} is invalid");
        }
    }

    public class KafkaRecieverOptionsValidator : IConfigurationValidator
    {
        private readonly KafkaReceiverOptions options;
        private readonly string name;
        public KafkaRecieverOptionsValidator(KafkaReceiverOptions options, string name)
        {
            this.options = options;
            this.name = name;
        }
        public void ValidateConfiguration()
        {
            if (options.TopicList == null || options.TopicList.Count == 0)
                throw new OrleansConfigurationException($"{nameof(KafkaReceiverOptions)} on stream provider {this.name} is invalid. {nameof(KafkaReceiverOptions.TopicList)} is invalid");
            if (string.IsNullOrEmpty(options.ConsumerGroup))
                throw new OrleansConfigurationException($"{nameof(KafkaReceiverOptions)} on stream provider {this.name} is invalid. {nameof(KafkaReceiverOptions.ConsumerGroup)} is invalid");
            if (options.TotalQueueCount <= 0)
                throw new OrleansConfigurationException($"{nameof(KafkaReceiverOptions)} on stream provider {this.name} is invalid. {nameof(KafkaReceiverOptions.TotalQueueCount)} is invalid");
        }
    }

}


