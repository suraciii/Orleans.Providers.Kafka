using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Newtonsoft.Json;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaStreamProviderConfig
    {
        public const int CacheSizeDefaultValue = 2048;
        public KafkaStreamProviderConfig(IProviderConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            SetKafkaConfig(config);
            TopicName = config.Properties["TopicName"];
            if (string.IsNullOrEmpty(TopicName)) throw new ArgumentNullException(nameof(TopicName));
            NumOfQueues = config.GetIntProperty("NumOfQueues", 8);
            Timeout = config.GetTimeSpanProperty("Timeout", TimeSpan.FromSeconds(3));
        }

        public IEnumerable<KeyValuePair<string, object>> KafkaConfig { get; private set; }
        public string TopicName { get; private set; }
        public int NumOfQueues { get; private set; }
        public TimeSpan Timeout { get; private set; }

        private void SetKafkaConfig(IProviderConfiguration config)
        {
            var kafkaConfig = JsonConvert.DeserializeObject<Dictionary<string, object>>(config.Properties["kafka"]);
            if (config.Properties.ContainsKey("kafka.topic"))
            {
                var topicConfig = JsonConvert.DeserializeObject<Dictionary<string, object>>(config.Properties["kafka.topic"]);
                kafkaConfig.Add("default.topic.config", topicConfig);
            }
            KafkaConfig = kafkaConfig;
        }
    }
}
