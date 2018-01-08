using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Orleans.Providers.Kafka.Streams
{
    [Serializable]
    public class KafkaStreamProviderException: Exception
    {
        public KafkaStreamProviderException()
        {
        }

        public KafkaStreamProviderException(string message)
            : base(message)
        {
        }

        public KafkaStreamProviderException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected KafkaStreamProviderException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
