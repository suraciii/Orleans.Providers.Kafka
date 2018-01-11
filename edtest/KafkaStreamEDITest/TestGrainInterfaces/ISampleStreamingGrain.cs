using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KafkaStreamEDITest.TestGrainInterfaces
{
    public interface ISampleProducerGrain : IGrainWithGuidKey
    {
        Task BecomeProducer(Guid streamId, string streamNamespace, string providerToUse);
        Task<string> GetMessageProduced();
        Task Produce(string msg);
    }

    public interface ISampleConsumerGrain : IGrainWithGuidKey
    {
        Task BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse);

        Task StopConsuming();

        Task<string> GetMessageConsumed();
    }

}
