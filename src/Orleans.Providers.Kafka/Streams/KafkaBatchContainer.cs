using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Providers.Kafka.Streams
{
    public class KafkaBatchContainer : IBatchContainer
    {
        private static readonly byte[] zero8 = { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 };

        public Guid StreamGuid { get; set; }

        public string StreamNamespace { get; set; }

        public StreamSequenceToken SequenceToken => EventSequenceToken;

        public EventSequenceToken EventSequenceToken { get; set; }

        public TopicPartitionOffset TopicPartitionOffset { get; set; }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return Events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, EventSequenceToken.CreateSequenceTokenForEvent(i)));
        }

        public List<Event> Events { get; set; }

        public bool ImportRequestContext()
        {
            return false;
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            return true;
        }

        public static KafkaBatchContainer FromKafkaMessage(Message msg, SerializationManager serializationManager, long seqNumber)
        {
            var container = new KafkaBatchContainer();
            var events = serializationManager.DeserializeFromByteArray<List<Event>>(msg.Value);
            var aggIdString = msg.Key == null ? null : Encoding.UTF8.GetString(msg.Key);
            if(string.IsNullOrEmpty(aggIdString) && Guid.TryParse(aggIdString, out var guid))
            {
                container.StreamGuid = guid;
            }
            else
            {
                container.StreamGuid = new Guid(msg.Partition, 0, 0, zero8);
            }

            container.StreamNamespace = msg.Topic;
            container.EventSequenceToken = new EventSequenceToken(seqNumber);
            container.Events = events;

            container.TopicPartitionOffset = msg.TopicPartitionOffset;

            return container;
        }

    }

    public class DomainEvent
    {
        public string AggregateId { get; set; }
        public Event Event { get; set; }
    }

    public class IntegrationEvent
    {
        public Event Event { get; set; }
    }

    [Serializable]
    [Bond.Schema]
    public class Event
    {
        [Bond.Id(0)]
        public string EventType { get; set; }
        [Bond.Id(1)]
        public byte[] Payload { get; set; }
    }

}
