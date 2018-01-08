using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaStreamProviderConfig
    {

        public KafkaStreamProviderConfig(IProviderConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            KafkaConfig = config.Properties.Select(kvp=>new KeyValuePair<string, object>(kvp.Key, kvp.Value));
        }

        public IEnumerable<KeyValuePair<string, object>> KafkaConfig { get; private set; }
        public string TopicName { get; private set; }

    }
}
