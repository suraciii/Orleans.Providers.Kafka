using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapterFactory : IQueueAdapterFactory
    {

        private KafkaStreamProviderConfig _config;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        private string _providerName;
        private ILogger _logger;
        private KafkaAdapter _adapter;
        private SerializationManager _serializationManager;


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
    }
}
