using Bond;
using Bond.IO.Unsafe;
using Bond.Protocols;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Streams;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.Provider.Kafka.Test
{
    public class SerializationManagerTests: BaseTestHost
    {
        private static Serializer<SimpleBinaryWriter<OutputBuffer>> serializer = new Serializer<SimpleBinaryWriter<OutputBuffer>>(typeof(List<Event>));
        private static Deserializer<SimpleBinaryReader<InputBuffer>> deserializer = new Deserializer<SimpleBinaryReader<InputBuffer>>(typeof(List<Event>));

        [Fact]
        public void EventSerializtionRountTrip()
        {
            ////var silo = await InitTestHost();

            ////var sm = silo.Services.GetRequiredService<SerializationManager>();

            //var evt = new Event
            //{
            //    EventType = "AAA",
            //    Payload = new byte[8]
            //};

            //var output = new OutputBuffer();
            //var bondWriter = new SimpleBinaryWriter<OutputBuffer>(output);
            //serializer.Serialize(evt, bondWriter);
            //var data = output.Data.ToArray();

            //var input = new InputBuffer(data);
            //var bondReader = new SimpleBinaryReader<InputBuffer>(input);
            //var result = deserializer.Deserialize<Event>(bondReader);

            //result.Should().BeEquivalentTo(evt);
        }

        [Fact]
        public void EventListSerializtionRountTrip()
        {

            var evt = new Event
            {
                EventType = "AAA",
                Payload = new byte[8]
            };

            var batch = new KafkaBatchContainer { Events = new List<Event> { evt } };

            var output = new OutputBuffer();
            var bondWriter = new SimpleBinaryWriter<OutputBuffer>(output);
            Serialize.To(bondWriter, batch);
            var data = output.Data.ToArray();


            var input = new InputBuffer(data);
            var bondReader = new SimpleBinaryReader<InputBuffer>(input);
            var result = Deserialize<KafkaBatchContainer>.From(bondReader);

            result.Events.Should().BeEquivalentTo(batch.Events);

        }
    }
}
