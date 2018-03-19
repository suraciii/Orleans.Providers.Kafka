using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using System;

namespace Orleans.Providers.Kafka.Streams
{
    internal class KafkaSequenceToken : EventSequenceToken
    {
        public string Topic { get; set; }
        public int Partition { get; set; }
        public long Offset { get; set; }

        public KafkaSequenceToken(string topic, int partition, long offset, int eventInd) : base(offset, eventInd)
        {
            throw new NotImplementedException();
        }

        //public KafkaSequenceToken(long seqNumber, int eventInd) : base(seqNumber, eventInd)
        //{
        //    // todo:
        //    throw new NotImplementedException();
        //}
    }
}