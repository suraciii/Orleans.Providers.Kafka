using Newtonsoft.Json;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Providers.Kafka.Streams
{
    public static class KafkaConfigurationExtensions
    {

        public static void AddKafkaStreamProvider(
            this ClusterConfiguration config,
            string providerName,
            string topicName,
            int NameOfQueues,
            Dictionary<string, object> kafkaConfig
            )
        {
            var properties = GetKafkaStreamProviderProperties(topicName, NameOfQueues, kafkaConfig);
            config.Globals.RegisterStreamProvider<KafkaStreamProvider>(providerName, properties);
        }

        public static void AddKafkaStreamProvider(
            this ClientConfiguration config,
            string providerName,
            string topicName,
            int NameOfQueues,
            Dictionary<string, object> kafkaConfig
            )
        {
            var properties = GetKafkaStreamProviderProperties(topicName, NameOfQueues, kafkaConfig);
            config.RegisterStreamProvider<KafkaStreamProvider>(providerName, properties);
        }

        private static Dictionary<string, string> GetKafkaStreamProviderProperties(
            string topicName,
            int NameOfQueues,
            Dictionary<string, object> oldKafkaConfig)
        {
            var kafkaConfig = new Dictionary<string, object>(oldKafkaConfig);
            var properties = new Dictionary<string, string>
            {
                {"TopicName", topicName },
                {"NumOfQueues", NameOfQueues.ToString() }
            };

            if(kafkaConfig.TryGetValue("default.topic.config", out var topicConfig))
            {
                kafkaConfig.Remove("default.topic.config");
                properties.Add("kafka.topic", JsonConvert.SerializeObject(topicConfig));
            }
            properties.Add("kafka", JsonConvert.SerializeObject(kafkaConfig));

            return properties;
        }
    }
}
