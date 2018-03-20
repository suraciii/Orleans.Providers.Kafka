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
namespace Orleans.Provider.Kafka.Test
{
    public class BaseTestHost
    {
        private int siloPort = 11111;
        private int gatewayPort = 30000;
        private ISiloHost host;

        protected virtual async Task<ISiloHost> InitTestHost()
        {
            var siloAddress = IPAddress.Loopback;
            var builder = new SiloHostBuilder()
                .UseDevelopmentClustering(options => options.PrimarySiloEndpoint = new IPEndPoint(siloAddress, siloPort))
                .ConfigureEndpoints(siloAddress, siloPort, gatewayPort)
                .ConfigureServices(ConfigureServices)
                .AddKafkaEventBusStreams("EventBus",
                kafka=>
                {
                    kafka.BrokerList = "kafka1:9094,kafka2:9094";
                }, 
                receiver=>
                {
                    receiver.ConsumerGroup = "test";
                    receiver.TopicList = new List<string> { "testA", "testB" };
                    receiver.TotalQueueCount = 8;
                });

            host = builder.Build();
            await host.StartAsync();
            return host;
        }

        protected virtual async Task<IClusterClient> CreateClient()
        {
            var siloAddress = IPAddress.Loopback;
            var gatewayPort = 30000;
            var client = new ClientBuilder()
                .UseStaticClustering(options => options.Gateways = new List<Uri>() { (new IPEndPoint(siloAddress, gatewayPort)).ToGatewayUri() })
                .ConfigureServices(services =>
                {
                    services.Configure<SerializationProviderOptions>(options =>
                    {
                        options.SerializationProviders.Add(typeof(BondSerializer).GetTypeInfo());
                    });
                })
                .Build();
            await client.Connect();
            return client;
        }

        private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            services.Configure<SerializationProviderOptions>(options =>
            {
                options.SerializationProviders.Add(typeof(BondSerializer).GetTypeInfo());
            });

        }

    }
}
