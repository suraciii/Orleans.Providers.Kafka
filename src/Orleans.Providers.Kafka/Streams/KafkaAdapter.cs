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
        private readonly IStreamQueueMapper _streamQueueMapper;
        // private readonly ProtocolGateway _gateway;
        private readonly IKafkaMapper _mapper;
        private readonly ILogger _logger;
        private readonly KafkaStreamProviderConfig _config;
        private readonly Producer _producer;

        public string Name { get; }
        public bool IsRewindable => true;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;


        public KafkaAdapter( KafkaStreamProviderConfig config,
            string providerName, ILogger logger, IStreamQueueMapper streamQueueMapper, IKafkaMapper mapper)
        {
            if (string.IsNullOrEmpty(providerName)) throw new ArgumentNullException(nameof(providerName));

            Name = providerName;
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _streamQueueMapper = streamQueueMapper ?? throw new ArgumentNullException(nameof(streamQueueMapper));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _producer = new Producer(config.KafkaConfig);

            _logger.LogInformation("KafkaAdapter - Created a KafkaAdapter");
        }



        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return KafkaAdapterReceiver.Create(_config, _logger, queueId, Name, _mapper);
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            throw new NotImplementedException();
        }

    }
}
