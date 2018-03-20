using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Serialization;
using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Tester.Grains;

namespace Tester
{
    public class BaseTestHost
    {
        private ISiloHost host;

        protected virtual async Task<ISiloHost> InitTestHost()
        {
            var builder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .ConfigureServices(ConfigureServices)
                .ConfigureApplicationParts(parts =>
                    parts.AddApplicationPart(typeof(SampleGrain).Assembly)
                    .WithReferences())
                .AddKafkaEventBusStreams(EventBusConstants.EVENT_BUS_PROVIDER,
                    kafka =>
                    {
                        kafka.BrokerList = "vm0:9094";
                    },
                    receiver =>
                    {
                        receiver.ConsumerGroup = "test";
                        receiver.TopicList = new List<string> { EventBusConstants.DOMAIN_EVENT_TEST_TOPIC };
                        receiver.TotalQueueCount = 4;
                    }
                );

            host = builder.Build();
            await host.StartAsync();
            return host;
        }

        protected virtual async Task<IClusterClient> CreateClient()
        {
            var client = new ClientBuilder()
                .UseLocalhostClustering()
                .ConfigureServices(services =>
                {
                    services.Configure<SerializationProviderOptions>(options =>
                    {
                        //options.SerializationProviders.Add(typeof(BondSerializer).GetTypeInfo());
                    });
                })
                .ConfigureApplicationParts(parts =>
                    parts.AddApplicationPart(typeof(SampleGrain).Assembly)
                    .WithReferences())
                .AddKafkaEventBusStreams(EventBusConstants.EVENT_BUS_PROVIDER,
                kafka =>
                {
                    kafka.BrokerList = "kafka1:9094,kafka2:9094";
                    kafka.Direction = Orleans.Streams.StreamProviderDirection.WriteOnly;
                })
                .Build();
            await client.Connect();
            return client;
        }

        private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            services.Configure<SerializationProviderOptions>(options =>
            {
                //options.SerializationProviders.Add(typeof(BondSerializer).GetTypeInfo());
            });

        }

    }
}