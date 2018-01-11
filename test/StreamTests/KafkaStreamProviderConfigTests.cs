
using Newtonsoft.Json;
using Orleans.Providers.Kafka.Streams;
using Orleans.Runtime.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;

namespace StreamTests
{
    public class KafkaStreamProviderConfigTests
    {
        [Fact]
        public void KafkaParseTest()
        {
            var kafkaConfig = new Dictionary<string, object>
            {
                { "group.id", "group" },
                { "enable.auto.commit", false },
                { "auto.commit.interval.ms", 5000 },
                //{ "statistics.interval.ms", 60000 },
                { "bootstrap.servers", "127.0.0.1:9092" },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            var mi = typeof(KafkaConfigurationExtensions).GetMethod("GetKafkaStreamProviderProperties", BindingFlags.NonPublic | BindingFlags.Static);
            var properties = mi.Invoke(null, new object[] { "Topic", 8, kafkaConfig }) as Dictionary<string, string>;

            var pc = new ProviderConfiguration(properties, "type", "name");

            var providerConfig = new KafkaStreamProviderConfig(pc);

            var topicConfig = providerConfig.KafkaConfig.First(kvp => kvp.Key == "default.topic.config").Value as IEnumerable<KeyValuePair<string, object>>;
            Assert.Equal((kafkaConfig["default.topic.config"] as Dictionary<string, object>)["auto.offset.reset"], topicConfig.First(kvp => kvp.Key == "auto.offset.reset").Value);
            Assert.Equal(JsonConvert.SerializeObject(kafkaConfig),
                JsonConvert.SerializeObject(providerConfig.KafkaConfig));
        }

    }
}
