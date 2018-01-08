using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapterFactory : IQueueAdapterFactory, IQueueAdapter
    {

        private KafkaStreamProviderConfig _config;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        private string _providerName;
        private ILogger _logger;
        private KafkaAdapter _adapter;
        //private SerializationManager _serializationManager;
        private readonly Producer _producer;

        public SerializationManager SerializationManager { get; private set; }

        public KafkaAdapterFactory()
        {

        }


        #region Factory

        public void Init(IProviderConfiguration config, string providerName, IServiceProvider serviceProvider)
        {
            throw new NotImplementedException();
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            throw new NotImplementedException();
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            throw new NotImplementedException();
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            throw new NotImplementedException();
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Adapter

        public string Name { get; }
        public bool IsRewindable => true;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            throw new NotImplementedException();
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            var partitionId = (int)queueId.GetNumericId();
            _logger.LogDebug("KafkaAdapter - For StreamId: {0}, StreamNamespace:{1} using partition {2}", streamGuid, streamNamespace, partitionId);

            var payload = KafkaBatchContainer.ToKafkaData(this.SerializationManager, streamGuid, streamNamespace, events, requestContext);

            var msg = await _producer.ProduceAsync(_config.TopicName, streamGuid.ToByteArray(), payload);

            if(msg.Error.HasError)
            {
                _logger.LogWarning("KafkaQueueAdapter - Error sending message through kafka client, the error code is {0}, message offset is {1}, reason: {2}", msg.Error.Code, msg.Offset, msg.Error.Reason);
                throw new StreamEventDeliveryFailureException("Producing message failed.");
            }
        }

        #endregion

    }
}
