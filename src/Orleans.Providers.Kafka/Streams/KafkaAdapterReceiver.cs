using Confluent.Kafka;
using Microsoft.Extensions.Logging;
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
        private readonly KafkaStreamProviderConfig _config;
        private readonly IKafkaMapper _mapper;
        private readonly ILogger _logger;

        public QueueId Id { get; }

        public static IQueueAdapterReceiver Create(KafkaStreamProviderConfig config, ILogger logger, QueueId queueId, string providerName, IKafkaMapper mapper)
        {
            return new KafkaAdapterReceiver(config, logger, queueId, providerName, mapper);
        }

        public KafkaAdapterReceiver(KafkaStreamProviderConfig config, ILogger logger, QueueId queueId, string providerName, IKafkaMapper mapper)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            Id = queueId ?? throw new ArgumentNullException(nameof(queueId));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _consumer = new Consumer(config.KafkaConfig);
        }

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
