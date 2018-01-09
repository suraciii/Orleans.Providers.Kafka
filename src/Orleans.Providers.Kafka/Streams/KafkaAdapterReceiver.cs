using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly Consumer _consumer;
        private readonly KafkaStreamProviderConfig _config;
        private readonly ILogger _logger;
        private long currentOffset;
        private Confluent.Kafka.Metadata metadata;
        private TopicPartitionOffset position;

        public QueueId Id { get; }

        public static IQueueAdapterReceiver Create(KafkaStreamProviderConfig config, ILogger logger, QueueId queueId, string providerName)
        {
            return new KafkaAdapterReceiver(config, logger, queueId, providerName);
        }

        public KafkaAdapterReceiver(KafkaStreamProviderConfig config, ILogger logger, QueueId queueId, string providerName)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            Id = queueId ?? throw new ArgumentNullException(nameof(queueId));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _consumer = new Consumer(config.KafkaConfig);
        }

        public Task Initialize(TimeSpan timeout)
        {
            var partitionId = (int)Id.GetNumericId();
            var tp = new TopicPartition(_config.TopicName, partitionId);
            _consumer.Assign(new List<TopicPartition> { tp });
            var po = _consumer.Position(_consumer.Assignment).First();
            if(po.Error.HasError)
            {
                _logger.LogWarning("KafkaAdapterReceiver - fetch position failed, the error code is {0}, reason: {1}", po.Error.Code, po.Error.Reason);
                throw new KafkaStreamProviderException("Fetch position failed.");
            }
            position = po.TopicPartitionOffset;
            return Task.CompletedTask;
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
