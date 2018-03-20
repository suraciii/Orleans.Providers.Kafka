using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Tester.Grains;
using Xunit;

namespace Tester
{
    public class EventBusTests: BaseTestHost
    {
        [Fact]
        public async Task PublishEventFromClient()
        {
            var silo = await InitTestHost();
            var client = await CreateClient();

            var grainId = Guid.NewGuid();
            var provider = client.GetStreamProvider(EventBusConstants.EVENT_BUS_PROVIDER);
            var stream = provider.GetStream<DomainEvent>(Guid.Empty, EventBusConstants.DOMAIN_EVENT_TEST_TOPIC);
            var msg = Guid.NewGuid().ToString();
            var domainEvent = new DomainEvent
            {
                AggregateId = grainId.ToString(),
                Event = new Event
                {
                    EventType = nameof(PublishEventFromClient),
                    Payload = Encoding.UTF8.GetBytes(msg)
                }
            };
            await stream.OnNextAsync(domainEvent);
            //await Task.Delay(-1);

            await Task.Delay(2000);


            var grain = client.GetGrain<ISampleGrain>(grainId);

            var latestMsg = await grain.GetLatestMessage();

            Assert.Equal(msg, latestMsg);
        }
    }
}
