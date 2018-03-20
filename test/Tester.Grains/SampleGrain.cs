using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Tester.Grains
{
    [ImplicitStreamSubscription(EventBusConstants.DOMAIN_EVENT_TEST_TOPIC)]
    public class SampleGrain: Grain, ISampleGrain
    {
        private string latestMessage = "";
        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("EventBus");
            var stream = streamProvider.GetStream<Event>(this.GetPrimaryKey(), EventBusConstants.DOMAIN_EVENT_TEST_TOPIC);
            await stream.SubscribeAsync((evt, t) => {
                switch (evt.EventType)
                {
                    case DomainEventConstants.PublishEventFromClient:
                        return HandlePublishEventFromClient(evt.Payload);
                    default:
                        throw new ArgumentOutOfRangeException(nameof(evt.EventType));
                }
            });
        }

        public Task<string> GetLatestMessage()
        {
            return Task.FromResult(latestMessage);
        }

        private Task HandlePublishEventFromClient(byte[] payload)
        {
            this.latestMessage = Encoding.UTF8.GetString(payload);
            return Task.CompletedTask;
        }
    }
}
