using Bond;
using Bond.IO.Unsafe;
using Bond.Protocols;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BatchSerializer = Bond.Serializer<Bond.Protocols.SimpleBinaryWriter<Bond.IO.Unsafe.OutputBuffer>>;
namespace Orleans.Streams
{
    public class KafkaAdapterFactory : IQueueAdapterFactory, IQueueAdapter
    {
        private static BatchSerializer serializer = new BatchSerializer(typeof(KafkaBatchContainer));

        private readonly ILoggerFactory loggerFactory;
        protected ILogger logger;
        protected IServiceProvider serviceProvider;
        private KafkaOptions kafkaOptions;
        private KafkaStreamCachePressureOptions cacheOptions;
        private KafkaReceiverOptions receiverOptions;
        private StreamStatisticOptions statisticOptions;
        private SimpleQueueAdapterCache _adapterCache;
        private IStreamQueueMapper streamQueueMapper;
        private ConcurrentDictionary<QueueId, KafkaAdapterReceiver> receivers;
        private Producer producer;
        private ITelemetryProducer telemetryProducer;
        public SerializationManager SerializationManager { get; private set; }

        public string Name { get; }

        private IStreamQueueCheckpointerFactory checkpointerFactory;

        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { get; set; }

        protected Func<IStreamQueueMapper> QueueMapperFactory { get; set; }

        public StreamProviderDirection Direction { get; }

        public bool IsRewindable => true;



        public KafkaAdapterFactory(string name, KafkaOptions kafkaOptions, KafkaReceiverOptions receiverOptions, KafkaStreamCachePressureOptions cacheOptions, StreamStatisticOptions statisticOptions,
            IServiceProvider serviceProvider, SerializationManager serializationManager, ITelemetryProducer telemetryProducer, ILoggerFactory loggerFactory)
        {
            this.Name = name;
            this.statisticOptions = statisticOptions ?? throw new ArgumentNullException(nameof(statisticOptions));
            this.kafkaOptions = kafkaOptions ?? throw new ArgumentNullException(nameof(kafkaOptions));
            this.cacheOptions = cacheOptions ?? throw new ArgumentNullException(nameof(cacheOptions));
            this.receiverOptions = receiverOptions ?? throw new ArgumentNullException(nameof(receiverOptions));
            this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            this.SerializationManager = serializationManager ?? throw new ArgumentNullException(nameof(serializationManager));
            this.telemetryProducer = telemetryProducer ?? throw new ArgumentNullException(nameof(telemetryProducer));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

            this.Direction = kafkaOptions.Direction;
        }


        public virtual void Init()
        {
            this.receivers = new ConcurrentDictionary<QueueId, KafkaAdapterReceiver>();
            this.telemetryProducer = this.serviceProvider.GetService<ITelemetryProducer>();

            if(producer == null)
                InitKafkaProducer();

            _adapterCache = new SimpleQueueAdapterCache(new SimpleQueueCacheOptions { CacheSize = cacheOptions.CacheSize }, Name, loggerFactory);

            if (this.StreamFailureHandlerFactory == null)
            {
                //TODO: Add a queue specific default failure handler with reasonable error reporting.
                this.StreamFailureHandlerFactory = partition => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
            }

            if (this.QueueMapperFactory == null)
            {
                var h = new HashRingStreamQueueMapperOptions { TotalQueueCount = receiverOptions.TotalQueueCount };
                this.streamQueueMapper =  new HashRingBasedStreamQueueMapper(h, Name);

                this.QueueMapperFactory = () => new HashRingBasedStreamQueueMapper(h, this.Name);
            }

            this.logger = this.loggerFactory.CreateLogger($"{this.GetType().FullName}"); // join topics?
        }

        protected virtual void InitKafkaProducer()
        {
            var config = kafkaOptions.ToKafkaProducerConfig();
            producer = new Producer(config);
        }

        private void InitCheckpointerFactory()
        {
            this.checkpointerFactory = this.serviceProvider.GetRequiredServiceByName<IStreamQueueCheckpointerFactory>(this.Name);
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            if (this.streamQueueMapper == null)
            {
                this.streamQueueMapper = this.QueueMapperFactory();
            }
            return Task.FromResult(this as IQueueAdapter);
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            //TODO: CreateAdapter must be called first.  Figure out how to safely enforce this
            return this.streamQueueMapper;
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return this.StreamFailureHandlerFactory(queueId);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return _adapterCache;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null)
            {
                throw new NotImplementedException("EventHub stream provider currently does not support non-null StreamSequenceToken.");
            }

            var cnt = events.Count();

            if (cnt == 0)
                return;

            KafkaBatchContainer batch = new KafkaBatchContainer();
            byte[] val = null;
            byte[] key = null;
            if(events is IEnumerable<DomainEvent> des)
            {
                if (cnt > 1)
                    throw new NotSupportedException("DomainEvent should only produce one by one");
                var aid = des.First().AggregateId;
                if(string.IsNullOrEmpty(aid))
                    throw new NotSupportedException("DomainEvent must have aggregate id");

                key = Encoding.UTF8.GetBytes(aid);
                batch.Events = des.Select(de => de.Event).ToList();
            }
            else if(events is IEnumerable<IntegrationEvent> ies)
            {
                key = null;
                batch.Events = ies.Select(ie => ie.Event).ToList();
            }
            else
            {
                throw new NotSupportedException("Only support DomainEvent or IntegrationEvent");
            }

            var output = new OutputBuffer();
            var bondWriter = new SimpleBinaryWriter<OutputBuffer>(output);
            serializer.Serialize(batch, bondWriter);
            val = output.Data.ToArray();

            var msg = await this.producer.ProduceAsync(streamNamespace, key, val);
            if (msg.Error.HasError)
            {
                // handle err
                // already throwed
                //throw new Confluent.Kafka.KafkaException(msg.Error);
            }

        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return new KafkaAdapterReceiver(kafkaOptions, receiverOptions, SerializationManager);
        }

        public static KafkaAdapterFactory Create(IServiceProvider services, string name)
        {
            var kafkaOptions = services.GetOptionsByName<KafkaOptions>(name);
            var receiverOptions = services.GetOptionsByName<KafkaReceiverOptions>(name);
            var cacheOptions = services.GetOptionsByName<KafkaStreamCachePressureOptions>(name);
            var statisticOptions = services.GetOptionsByName<StreamStatisticOptions>(name);
            var factory = ActivatorUtilities.CreateInstance<KafkaAdapterFactory>(services, name, kafkaOptions, receiverOptions, cacheOptions, statisticOptions);
            factory.Init();
            return factory;
        }
    }

}
