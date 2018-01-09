using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaAdapterReceiver : IQueueAdapterReceiver, IQueueCache
    {
        private readonly Consumer _consumer;
        private readonly KafkaStreamProviderConfig _config;
        private readonly ILogger _logger;
        private long currentOffset;
        private Confluent.Kafka.Metadata metadata;
        private TopicPartitionOffset position;
        private readonly SerializationManager _serializationManager;

        public QueueId Id { get; }

        public static KafkaAdapterReceiver Create(KafkaStreamProviderConfig config, ILogger logger, QueueId queueId, string providerName, SerializationManager serializationManager)
        {
            return new KafkaAdapterReceiver(config, logger, queueId, providerName, serializationManager);
        }

        public KafkaAdapterReceiver(KafkaStreamProviderConfig config, ILogger logger, QueueId queueId, string providerName, SerializationManager serializationManager)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            Id = queueId ?? throw new ArgumentNullException(nameof(queueId));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _serializationManager = serializationManager ?? throw new ArgumentNullException(nameof(_serializationManager));

            _consumer = new Consumer(config.KafkaConfig);
        }

        #region AdapterReceiver

        public Task Initialize(TimeSpan timeout)
        {
            var partitionId = (int)Id.GetNumericId();
            var tp = new TopicPartition(_config.TopicName, partitionId);
            _consumer.Assign(new List<TopicPartition> { tp });
            var po = _consumer.Position(_consumer.Assignment).First();
            if (po.Error.HasError)
            {
                _logger.LogWarning("KafkaAdapterReceiver - fetch position failed, the error code is {0}, reason: {1}", po.Error.Code, po.Error.Reason);
                throw new KafkaStreamProviderException("Fetch position failed.");
            }
            position = po.TopicPartitionOffset;
            return Task.CompletedTask;
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            Task<List<Message>> fetchingTask = Task.Run(() =>
            {
                List<Message> messages = new List<Message>();
                for (int i = 0; i < maxCount; ++i)
                {
                    if (_consumer.Consume(out var msg, _config.Timeout))
                    {
                        messages.Add(msg);
                    }
                    else
                    {
                        break;
                    }
                }
                return messages;
            });
            await Task.WhenAny(fetchingTask, Task.Delay(_config.Timeout));
            if (!fetchingTask.IsCompleted)
            {
                if (fetchingTask.IsFaulted && fetchingTask.Exception != null)
                {
                    _logger.LogWarning("KafkaQueueAdapterReceiver - Fetching messages from kafka failed, tried to fetch {0} messages ",
                        maxCount);
                    throw fetchingTask.Exception;
                }
                _logger.LogWarning("KafkaQueueAdapterReceiver - Fetching messages from kafka timeout, tried to fetch {0} messages ",
                    maxCount);
                throw new KafkaStreamProviderException("Fetching messages from kafka timeout");
            }

            IList<IBatchContainer> batches = new List<IBatchContainer>();
            if (fetchingTask.Result == null)
            {
                return batches;
            }

            var messages = fetchingTask.Result;
            batches = messages.Select(m => KafkaBatchContainer.FromKafkaMessage(_serializationManager, m, m.Offset.Value)).ToList();
            if (batches.Count <= 0) return batches;

            _logger.LogDebug("KafkaQueueAdapterReceiver - Pulled {0} messages for queue number {1}", batches.Count, Id.GetNumericId());

            return batches;
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            if (messages.Any())
            {
                await CommitOffset();
            }
        }

        private async Task CommitOffset()
        {
            var commitTask = _consumer.CommitAsync();
            await Task.WhenAny(commitTask, Task.Delay(_config.Timeout));

            if (!commitTask.IsCompleted || commitTask.Result.Error.HasError)
            {
                var newException = new KafkaStreamProviderException("Commit offset operation has failed");

                _logger.LogError(newException,
                    "KafkaQueueAdapterReceiver - Commit offset operation has failed.");
                throw new KafkaStreamProviderException();
            }
            var offset = commitTask.Result.Offsets.Max(t => t.Offset.Value);
            _logger.LogTrace(
                "KafkaQueueAdapterReceiver - Commited an offset {0} to the ConsumerGroup", offset);
        }

        public Task Shutdown(TimeSpan timeout)
        {
            _consumer.Unassign();
            return Task.CompletedTask;
        }

        #endregion

        #region QueueCache

        public void AddToCache(IList<IBatchContainer> messages)
        {
            throw new NotImplementedException();
        }

        public bool TryPurgeFromCache(out IList<IBatchContainer> purgedItems)
        {
            throw new NotImplementedException();
        }

        public IQueueCacheCursor GetCacheCursor(IStreamIdentity streamIdentity, StreamSequenceToken token)
        {
            throw new NotImplementedException();
        }

        public bool IsUnderPressure()
        {
            throw new NotImplementedException();
        }

        public int GetMaxAddCount()
        {
            throw new NotImplementedException();
        }

        #endregion

    }
}
