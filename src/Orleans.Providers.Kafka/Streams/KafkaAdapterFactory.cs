using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapterFactory : IQueueAdapterFactory
    {

        private KafkaStreamProviderOptions _options;
        private HashRingBasedStreamQueueMapper _streamQueueMapper;
        private IQueueAdapterCache _adapterCache;
        private string _providerName;
        private Logger _logger;
        private KafkaQueueAdapter _adapter;
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
