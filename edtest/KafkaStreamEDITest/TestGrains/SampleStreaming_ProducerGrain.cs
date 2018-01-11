using KafkaStreamEDITest.TestGrainInterfaces;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KafkaStreamEDITest.TestGrains
{
    public class SampleProducerGrain : Grain, ISampleProducerGrain
    {
        private IAsyncStream<string> producer;
        private string message = "";

        public override Task OnActivateAsync()
        {
            return Task.CompletedTask;
        }

        public Task BecomeProducer(Guid streamId, string streamNamespace, string providerToUse)
        {
            IStreamProvider streamProvider = base.GetStreamProvider(providerToUse);
            producer = streamProvider.GetStream<string>(streamId, streamNamespace);
            return Task.CompletedTask;
        }

        public Task<string> GetMessageProduced()
        {
            return Task.FromResult(message);
        }

        public async Task Produce(string msg)
        {
            await producer.OnNextAsync(msg);
            message = msg;
        }

    }

    public class SampleConsumerGrain : Grain, ISampleConsumerGrain
    {
        private IAsyncObservable<string> consumer;
        private string message = "not assigned";
        private long offset;
        private StreamSubscriptionHandle<string> consumerHandle;

        public override Task OnActivateAsync()
        {
            consumerHandle = null;
            return Task.CompletedTask;
        }

        public async Task BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse)
        {
            IStreamProvider streamProvider = base.GetStreamProvider(providerToUse);
            consumer = streamProvider.GetStream<string>(streamId, streamNamespace);
            consumerHandle = await consumer.SubscribeAsync<string>((data, token) => 
            {
                this.message = data;
                this.offset = (token as EventSequenceToken).SequenceNumber;
                return Task.CompletedTask;
            });
        }

        public async Task StopConsuming()
        {
            if (consumerHandle != null)
            {
                await consumerHandle.UnsubscribeAsync();
                consumerHandle = null;
            }
        }

        public Task<string> GetMessageConsumed()
        {
            return Task.FromResult(message);
        }

        public override Task OnDeactivateAsync()
        {
            return Task.CompletedTask;
        }
    }

}
