using Confluent.Kafka;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly Consumer _consumer;
        public Task Initialize(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            throw new NotImplementedException();
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            throw new NotImplementedException();
        }

        public Task Shutdown(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}
