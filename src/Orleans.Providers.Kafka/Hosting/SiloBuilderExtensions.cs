using Orleans.Providers.Kafka.Streams;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Hosting
{
    public static class SiloBuilderExtensions
    {

        public static SiloKafkaStreamConfigurator AddKafkaStreams(
            this ISiloHostBuilder builder,
            string name)
        {
            return new SiloKafkaStreamConfigurator(name, builder);
        }

        public static ISiloHostBuilder AddKafkaStreams(
            this ISiloHostBuilder builder,
            string name,
            Action<SiloKafkaStreamConfigurator> configure)
        {
            configure?.Invoke(builder.AddKafkaStreams(name));
            return builder;
        }

        public static ISiloHostBuilder AddKafkaStreams(
            this ISiloHostBuilder builder,
            string name, Action<KafkaOptions> configureKafka, Action<KafkaReceiverOptions> configureReceiver)
        {
            builder.AddKafkaStreams(name)
                .ConfigureKafka(ob => ob.Configure(configureKafka))
                .ConfigureReceiver(ob => ob.Configure(configureReceiver));
            return builder;
        }
    }
}
