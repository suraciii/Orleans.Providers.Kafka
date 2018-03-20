using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orleans.Hosting
{
    public static class ClientBuilderExtensions
    {

        public static ClusterClientKafkaEventBusStreamConfigurator AddKafkaEventBusStreams(
            this IClientBuilder builder,
            string name)
        {
            return new ClusterClientKafkaEventBusStreamConfigurator(name, builder);
        }

        public static IClientBuilder AddKafkaEventBusStreams(
           this IClientBuilder builder,
           string name,
           Action<ClusterClientKafkaEventBusStreamConfigurator> configure)
        {
            configure?.Invoke(builder.AddKafkaEventBusStreams(name));
            return builder;
        }

        public static IClientBuilder AddKafkaEventBusStreams(
            this IClientBuilder builder,
            string name, Action<KafkaOptions> configureEventHub)
        {
            builder.AddKafkaEventBusStreams(name).ConfigureKafka(ob => ob.Configure(configureEventHub));
            return builder;
        }
    }
}
