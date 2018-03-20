using Bond;
using Bond.IO.Unsafe;
using Bond.Protocols;
using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using BatchDeserializer = Bond.Deserializer<Bond.Protocols.SimpleBinaryReader<Bond.IO.Unsafe.InputBuffer>>;
using BatchSerializer = Bond.Serializer<Bond.Protocols.SimpleBinaryWriter<Bond.IO.Unsafe.OutputBuffer>>;
namespace Orleans.Streams
{
    [Bond.Schema]
    public class KafkaBatchContainer : IBatchContainer
    {
        private static readonly byte[] zero8 = { 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0 };
        private static Serializer<SimpleBinaryWriter<OutputBuffer>> serializer = new Serializer<SimpleBinaryWriter<OutputBuffer>>(typeof(KafkaBatchContainer));
        private static Deserializer<SimpleBinaryReader<InputBuffer>> deserializer = new Deserializer<SimpleBinaryReader<InputBuffer>>(typeof(KafkaBatchContainer));

        [Bond.Id(0)]
        public List<Event> Events { get; set; }

        public Guid StreamGuid { get; set; }

        public string StreamNamespace { get; set; }

        public StreamSequenceToken SequenceToken => EventSequenceToken;

        public EventSequenceToken EventSequenceToken { get; set; }

        public TopicPartitionOffset TopicPartitionOffset { get; set; }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return Events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, EventSequenceToken.CreateSequenceTokenForEvent(i)));
        }

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

        public static KafkaBatchContainer FromKafkaMessage(Message msg, BatchDeserializer deserializer, long seqNumber)
        {

            var input = new InputBuffer(msg.Value);
            var bondReader = new SimpleBinaryReader<InputBuffer>(input);
            var container = deserializer.Deserialize<KafkaBatchContainer>(bondReader);

            var aggIdString = msg.Key == null ? null : Encoding.UTF8.GetString(msg.Key);
            if (string.IsNullOrEmpty(aggIdString) && Guid.TryParse(aggIdString, out var guid))
            {
                container.StreamGuid = guid;
            }
            else
            {
                container.StreamGuid = new Guid(msg.Partition, 0, 0, zero8);
            }

            container.StreamNamespace = msg.Topic;
            container.EventSequenceToken = new EventSequenceToken(seqNumber);

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

    [Bond.Schema]
    public class Event
    {
        [Bond.Id(0)]
        public string EventType { get; set; }
        [Bond.Id(1)]
        public byte[] Payload { get; set; }
    }

}
