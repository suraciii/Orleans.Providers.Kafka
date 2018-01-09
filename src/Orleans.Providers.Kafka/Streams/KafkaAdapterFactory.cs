using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapterFactory : IQueueAdapterFactory, IQueueAdapter
    {

        private IServiceProvider serviceProvider;
        private KafkaStreamProviderConfig config;
        private IStreamQueueMapper streamQueueMapper;
        private IQueueAdapterCache adapterCache;
        private string providerName;
        private ILogger logger;
        private ILoggerFactory loggerFactory;
        private SerializationManager serializationManager;
        private Producer producer;
        private Confluent.Kafka.Metadata metadata;

        #region Factory

        public void Init(IProviderConfiguration providerCfg, string providerName, IServiceProvider serviceProvider)
        {
            if (providerCfg == null) throw new ArgumentNullException(nameof(providerCfg));
            if (string.IsNullOrWhiteSpace(providerName)) throw new ArgumentNullException(nameof(providerName));

            this.providerName = providerName;
            this.config = new KafkaStreamProviderConfig(providerCfg);
            this.serviceProvider = serviceProvider;
            this.loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            this.serializationManager = this.serviceProvider.GetRequiredService<SerializationManager>();

            logger = this.loggerFactory.CreateLogger<KafkaAdapterFactory>();

            var cacheSize = SimpleQueueAdapterCache.ParseSize(providerCfg, KafkaStreamProviderConfig.CacheSizeDefaultValue);
            adapterCache = new SimpleQueueAdapterCache(cacheSize, providerName, loggerFactory);

            streamQueueMapper = new HashRingBasedStreamQueueMapper(config.NumOfQueues, providerName);
        }

        private void InitProducer()
        {
            producer = new Producer(config.KafkaConfig);
            metadata = producer.GetMetadata(false, config.TopicName, config.Timeout);
        } 

        public Task<IQueueAdapter> CreateAdapter()
        {
            InitProducer();
            return Task.FromResult<IQueueAdapter>(this);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return adapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return streamQueueMapper;
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler(false));
        }

        #endregion

        #region Adapter

        public string Name => providerName;
        public bool IsRewindable => true;
        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return KafkaAdapterReceiver.Create(config, logger, queueId, Name, serializationManager);
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            var partitionId = (int)queueId.GetNumericId();
            logger.LogDebug("KafkaAdapter - For StreamId: {0}, StreamNamespace:{1} using partition {2}", streamGuid, streamNamespace, partitionId);

            var payload = KafkaBatchContainer.ToKafkaData(this.serializationManager, streamGuid, streamNamespace, events, requestContext);

            var msg = await producer.ProduceAsync(config.TopicName, null, 0, 0, payload, 0, payload.Length, partitionId);

            if(msg.Error.HasError)
            {
                logger.LogWarning("KafkaQueueAdapter - Error sending message through kafka client, the error code is {0}, message offset is {1}, reason: {2}", msg.Error.Code, msg.Offset, msg.Error.Reason);
                throw new KafkaStreamProviderException("Producing message failed.");
            }
        }

        #endregion

    }
}
