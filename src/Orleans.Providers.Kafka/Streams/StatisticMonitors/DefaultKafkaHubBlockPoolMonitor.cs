using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Providers.Kafka.Streams.StatisticMonitors
{
    public class DefaultKafkaBlockPoolMonitor: DefaultBlockPoolMonitor
    {
        public DefaultKafkaBlockPoolMonitor(KafkaBlockPoolMonitorDimensions dimensions, ITelemetryProducer telemetryProducer)
         : base(telemetryProducer)
        {
            this.LogProperties = new Dictionary<string, string>
            {
                //{"Path", dimensions.EventHubPath},
                //{"ObjectPoolId", dimensions.BlockPoolId}
            };
        }
    }
}
