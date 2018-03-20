using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using System;

namespace Orleans.Streams
{
    public class SiloKafkaEventBusStreamConfigurator : SiloRecoverableStreamConfigurator
    {
        public SiloKafkaEventBusStreamConfigurator(string name, ISiloHostBuilder builder)
            : base(name, builder, KafkaEventBusAdapterFactory.Create)
        {
            this.siloBuilder.ConfigureApplicationParts(parts => parts.AddFrameworkPart(typeof(KafkaEventBusAdapterFactory).Assembly))
                .ConfigureServices(services => services.ConfigureNamedOptionForLogging<KafkaOptions>(name)
                    .ConfigureNamedOptionForLogging<KafkaReceiverOptions>(name)
                    .ConfigureNamedOptionForLogging<KafkaStreamCachePressureOptions>(name)
                    .AddTransient<IConfigurationValidator>(sp => new KafkaOptionsValidator(sp.GetOptionsByName<KafkaOptions>(name), name))
                    .AddTransient<IConfigurationValidator>(sp => new KafkaRecieverOptionsValidator(sp.GetOptionsByName<KafkaReceiverOptions>(name), name))
                );

            this.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
        }


        public SiloKafkaEventBusStreamConfigurator ConfigureKafka(Action<OptionsBuilder<KafkaOptions>> configureOptions)
        {
            this.Configure<KafkaOptions>(configureOptions);
            return this;
        }

        public SiloKafkaEventBusStreamConfigurator ConfigureReceiver(Action<OptionsBuilder<KafkaReceiverOptions>> configureOptions)
        {
            this.Configure<KafkaReceiverOptions>(configureOptions);
            return this;
        }

        public SiloKafkaEventBusStreamConfigurator ConfigureCachePressuring(Action<OptionsBuilder<KafkaStreamCachePressureOptions>> configureOptions)
        {
            this.Configure<KafkaStreamCachePressureOptions>(configureOptions);
            return this;
        }
    }


    public class ClusterClientKafkaEventBusStreamConfigurator : ClusterClientPersistentStreamConfigurator
    {
        public ClusterClientKafkaEventBusStreamConfigurator(string name, IClientBuilder builder)
           : base(name, builder, KafkaEventBusAdapterFactory.Create)
        {
            this.clientBuilder.ConfigureApplicationParts(parts => parts.AddFrameworkPart(typeof(KafkaEventBusAdapterFactory).Assembly))
                .ConfigureServices(services => services.ConfigureNamedOptionForLogging<KafkaOptions>(name)
                .AddTransient<IConfigurationValidator>(sp => new KafkaOptionsValidator(sp.GetOptionsByName<KafkaOptions>(name), name)));

            this.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
        }

        public ClusterClientKafkaEventBusStreamConfigurator ConfigureKafka(Action<OptionsBuilder<KafkaOptions>> configureOptions)
        {
            this.Configure<KafkaOptions>(configureOptions);
            return this;
        }
    }
}
