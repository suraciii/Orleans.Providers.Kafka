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
    public class KafkaAdapterReceiver : IQueueAdapterReceiver
    {
        private Consumer consumer;
        private long lastReadMessage;
        private SerializationManager serializationManager;
        private readonly KafkaOptions kafkaOptions;
        private readonly KafkaReceiverOptions receiverOptions;
        public KafkaAdapterReceiver(KafkaOptions kafkaOptions, KafkaReceiverOptions receiverOptions, SerializationManager serializationManager)
        {
            this.kafkaOptions = kafkaOptions;
            this.receiverOptions = receiverOptions;
            this.serializationManager = serializationManager;
        }

        public Task Initialize(TimeSpan timeout)
        {
            if (consumer != null) // check in case we already shut it down.
            {
                return InitializeInternal(timeout);
            }
            return Task.CompletedTask;
        }

        private Task InitializeInternal(TimeSpan timeout)
        {
            var config = receiverOptions.ToKafkaConsumerConfig(kafkaOptions);
            consumer = new Consumer(config);
            var meta = consumer.GetMetadata(false, timeout);
            consumer.Subscribe(receiverOptions.TopicList);
            return Task.CompletedTask;
        }

        public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            if(consumer == null)
            {
                IList<IBatchContainer> empty = new List<KafkaBatchContainer>().Cast<IBatchContainer>().ToList();
                return Task.FromResult(empty);
            }
            List<Message> msgs = new List<Message>();
            while(true)
            {
                consumer.Consume(out var msg, 50);

                if (msg.Error.Code == ErrorCode.Local_PartitionEOF)
                    break;

                if (!msg.Error.HasError)
                {
                    msgs.Add(msg);
                    if (maxCount != QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG && msgs.Count >= maxCount)
                        break;
                }
                else
                {
                    // handle
                    break;
                }
            }

            IList<IBatchContainer> batches = new List<IBatchContainer>();
            foreach (var msg in msgs)
            {
                IBatchContainer container = KafkaBatchContainer.FromKafkaMessage(msg, serializationManager, lastReadMessage++);
                batches.Add(container);
            }

            return Task.FromResult(batches);
        }

        public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            var tps = messages.Cast<KafkaBatchContainer>().Select(c => c.TopicPartitionOffset);
            return consumer.CommitAsync(tps);
        }

        public Task Shutdown(TimeSpan timeout)
        {
            // _currentCommitTask
            return Task.CompletedTask;
        }
    }
}
