using Confluent.Kafka;
using Orleans.Streams;
using System;
using System.Collections.Generic;

namespace Orleans.Providers.Kafka.Streams
{
    public interface IKafkaMapper
    {
        Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext);

        Message ToKafkaMessage<T>(Guid streamId, string streamNamespace, T singleEvent, Dictionary<string, object> requestContext);

        IBatchContainer FromKafkaMessage(Message kafkaMessage, long sequenceId);

        T MapToType<T>(Message kafkaMessage, long sequenceId);

    }
}