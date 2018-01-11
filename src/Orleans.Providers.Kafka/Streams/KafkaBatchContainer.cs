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
    [Serializable]
    public class KafkaBatchContainer : IBatchContainer
    {
        
        private EventSequenceToken sequenceToken;

        private readonly List<object> events;
        
        private readonly Dictionary<string, object> requestContext;

        public StreamSequenceToken SequenceToken => sequenceToken;
        
        public Guid StreamGuid { get; private set; }
        public string StreamNamespace { get; private set; }
        public string Timestamp { get; private set; }

        public KafkaBatchContainer(Guid streamGuid, String streamNamespace, List<object> events, Dictionary<string, object> requestContext)
        {
            StreamGuid = streamGuid;
            StreamNamespace = streamNamespace;
            this.events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events");
            this.requestContext = requestContext;
            Timestamp = DateTime.UtcNow.ToString("O");
        }

        internal static byte[] ToKafkaData<T>(SerializationManager serializationManager, Guid streamId, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var container = new KafkaBatchContainer(streamId, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var bytes = serializationManager.SerializeToByteArray(container);
            return bytes;
        }

        internal static IBatchContainer FromKafkaMessage(SerializationManager serializationManager, Message msg, long sequenceId)
        {
            
            var kafkaBatch = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(msg.Value);
            kafkaBatch.sequenceToken = new EventSequenceToken(sequenceId);

            return kafkaBatch;
        }

        public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
        {
            return events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, sequenceToken.CreateSequenceTokenForEvent(i)));
        }

        public bool ImportRequestContext()
        {
            if (requestContext != null)
            {
                RequestContextExtensions.Import(requestContext);
                return true;
            }
            return false;
        }

        public bool ShouldDeliver(IStreamIdentity stream, object filterData, StreamFilterPredicate shouldReceiveFunc)
        {
            return events.Any(item => shouldReceiveFunc(stream, filterData, item));
        }


    }
}
