using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Newtonsoft.Json;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaStreamProviderConfig
    {

        public KafkaStreamProviderConfig(IProviderConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            KafkaConfig = JsonConvert.DeserializeObject<IEnumerable<KeyValuePair<string, object>>>(config.Properties["kafka"]);
            TopicName = config.Properties["TopicName"];
            NumOfQueues = config.GetIntProperty("NumOfQueues", 8);
            Timeout = config.GetTimeSpanProperty("Timeout", TimeSpan.FromSeconds(3));
        }

        public IEnumerable<KeyValuePair<string, object>> KafkaConfig { get; private set; }
        public string TopicName { get; private set; }
        public int NumOfQueues { get; private set; }
        public TimeSpan Timeout { get; private set; }
    }
}
