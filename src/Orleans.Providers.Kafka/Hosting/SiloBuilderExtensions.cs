using Orleans.Configuration;
using Orleans.Streams;
using System;

namespace Orleans.Hosting
{
    public static class SiloBuilderExtensions
    {

        public static SiloKafkaStreamConfigurator AddKafkaEventBusStreams(
            this ISiloHostBuilder builder,
            string name)
        {
            return new SiloKafkaStreamConfigurator(name, builder);
        }

        public static ISiloHostBuilder AddKafkaEventBusStreams(
            this ISiloHostBuilder builder,
            string name,
            Action<SiloKafkaStreamConfigurator> configure)
        {
            configure?.Invoke(builder.AddKafkaEventBusStreams(name));
            return builder;
        }

        public static ISiloHostBuilder AddKafkaEventBusStreams(
            this ISiloHostBuilder builder,
            string name, Action<KafkaOptions> configureKafka, Action<KafkaReceiverOptions> configureReceiver)
        {
            builder.AddKafkaEventBusStreams(name)
                .ConfigureKafka(ob => ob.Configure(configureKafka))
                .ConfigureReceiver(ob => ob.Configure(configureReceiver));
            return builder;
        }
    }
}
