using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapter : IQueueAdapter
    {
        private readonly IStreamQueueMapper _streamQueueMapper;
        // private readonly ProtocolGateway _gateway;
        private readonly IKafkaMapper _mapper;
        private readonly ILogger _logger;
        private readonly KafkaStreamProviderConfig _config;
        private readonly Producer _producer;

        public SerializationManager SerializationManager { get; private set; }
        public string Name { get; }
        public bool IsRewindable => true;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;


        public KafkaAdapter(KafkaStreamProviderConfig config,
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

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            await Task.Run(() => QueueMessageBatchExternal<T>(streamGuid, streamNamespace, events, token, requestContext));
        }

        private void QueueMessageBatchExternal<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            var partitionId = (int)queueId.GetNumericId();
            _logger.LogDebug("KafkaAdapter - For StreamId: {0}, StreamNamespace:{1} using partition {2}", streamGuid, streamNamespace, partitionId);

            var payload = KafkaBatchContainer.ToKafkaData(streamGuid, streamNamespace, events, requestContext);

        }

    }
}
