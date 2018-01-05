using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapter: IQueueAdapter
    {
        private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
        // private readonly ProtocolGateway _gateway;
        // private readonly IKafkaBatchFactory _batchFactory;
        private readonly ILogger _logger;
        private readonly KafkaStreamProviderConfig _config;
        private readonly Producer _producer;

        public string Name { get; }
        public bool IsRewindable => true;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;


        public KafkaAdapter(HashRingBasedStreamQueueMapper queueMapper, KafkaStreamProviderConfig config,
            string providerName, ILogger logger)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            //if (batchFactory == null) throw new ArgumentNullException(nameof(batchFactory));
            if (queueMapper == null) throw new ArgumentNullException(nameof(queueMapper));
            if (logger == null) throw new ArgumentNullException(nameof(logger));
            if (string.IsNullOrEmpty(providerName)) throw new ArgumentNullException(nameof(providerName));

            _config = config;
            _streamQueueMapper = queueMapper;
            Name = providerName;
            //_batchFactory = batchFactory;
            _logger = logger;

            _producer = new Producer(config.KafkaConfig);

            _logger.LogInformation("KafkaAdapter - Created a KafkaAdapter");
        }



        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            throw new NotImplementedException();
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            throw new NotImplementedException();
        }

    }
}
