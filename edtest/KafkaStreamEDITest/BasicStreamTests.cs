using KafkaStreamEDITest.TestGrainInterfaces;
using KafkaStreamEDITest.TestGrains;
using Orleans;
using Orleans.Hosting;
using Orleans.Providers.Kafka.Streams;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace KafkaStreamEDITest
{
    public class BasicStreamTests
    {
        private const string providerName = "TestKafkaStreamProvider";
        private const string topicName = nameof(BasicStreamTests) + "Topic";
        private const int numOfQueues = 8;
        public const string brokerList = "default:9092";
        private readonly TestServer _server;
        private readonly IClusterClient _client;
        public BasicStreamTests()
        {
            var kafkaConfig = new Dictionary<string, object>
            {
                { "group.id", nameof(BasicStreamTests)+"Group" },
                { "enable.auto.commit", false },
                { "auto.commit.interval.ms", 5000 },
                { "statistics.interval.ms", 60000 },
                { "bootstrap.servers", brokerList },
                { "default.topic.config", new Dictionary<string, object>()
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            var clusterConfig = ClusterConfiguration.LocalhostPrimarySilo();
            clusterConfig.AddKafkaStreamProvider(providerName, topicName, numOfQueues, kafkaConfig);
            clusterConfig.AddMemoryStorageProvider();
            var clusterBuilder = new SiloHostBuilder()
                .UseConfiguration(clusterConfig)
                .ConfigureApplicationParts(parts => parts.AddFromAppDomain().AddFromApplicationBaseDirectory());
            _server = new TestServer(clusterBuilder);
            var host = _server.Host;

            var clientConfig = ClientConfiguration.LocalhostSilo();
            clientConfig.AddKafkaStreamProvider(providerName, topicName, numOfQueues, kafkaConfig);
            var clientBuilder = new ClientBuilder()
                .UseConfiguration(clientConfig)
                .ConfigureApplicationParts(parts => parts.AddFromAppDomain().AddFromApplicationBaseDirectory());
            _client = _server.CreateClient(clientBuilder);
        }

        [Fact]
        public async Task ProduceTest()
        {
            var streamId = Guid.NewGuid();
            var produceGrain = _client.GetGrain<ISampleProducerGrain>(Guid.Empty);
            await produceGrain.BecomeProducer(streamId, null, providerName);
            var msg = "test";
            await produceGrain.Produce(msg);

            var getMsgTask = Task.Run(async () =>
            {
                while (true)
                {
                    var rmsg = await produceGrain.GetMessageProduced();
                    if (rmsg == msg) return;
                }
            });

            await Task.WhenAny(getMsgTask, Task.Delay(1000));

            Assert.True(getMsgTask.IsCompleted);
        }

        [Fact]
        public void BasicTest()
        {

        }
    }
}
