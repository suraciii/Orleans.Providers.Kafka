using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Kafka.Streams;
using Orleans.Serialization;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.Provider.Kafka.Test
{
    public class SerializationManagerTests: BaseTestHost
    {
        [Fact]
        public async Task EventSerializtionRountTrip()
        {
            var silo = await InitTestHost();

            var sm = silo.Services.GetRequiredService<SerializationManager>();

            var evt = new Event
            {
                EventType = "AAA",
                Payload = new byte[8]
            };

            sm.RoundTripSerializationForTesting(evt).Should().BeEquivalentTo(evt);
        }

        [Fact]
        public async Task EventListSerializtionRountTrip()
        {
            var silo = await InitTestHost();

            var sm = silo.Services.GetRequiredService<SerializationManager>();

            var evt = new Event
            {
                EventType = "AAA",
                Payload = new byte[8]
            };

            var eventList = new List<Event> { evt };

            sm.RoundTripSerializationForTesting(eventList).Should().BeEquivalentTo(eventList);

            var data = sm.SerializeToByteArray(eventList);
            var el = sm.DeserializeFromByteArray<List<Event>>(data);
        }
    }
}
