using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaTutorialBasic;

namespace KaffkaTutorialBasic
{
    public class Program
    {
        static string ToString(int[] array) => $"[{string.Join(", ", array)}]";
        static async Task CreateTopicAsync(string bootstrapServers, string topicName)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                       new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 3 } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }
        static void PrintMetadata(string bootstrapServers)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // Warning: The API for this functionality is subject to change.
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                Console.WriteLine($"{meta.OriginatingBrokerId} {meta.OriginatingBrokerName}");
                meta.Brokers.ForEach(broker =>
                    Console.WriteLine($"Broker: {broker.BrokerId} {broker.Host}:{broker.Port}"));

                meta.Topics.ForEach(topic =>
                {
                    Console.WriteLine($"Topic: {topic.Topic} {topic.Error}");
                    topic.Partitions.ForEach(partition =>
                    {
                        Console.WriteLine($"  Partition: {partition.PartitionId}");
                        Console.WriteLine($"    Replicas: {ToString(partition.Replicas)}");
                        Console.WriteLine($"    InSyncReplicas: {ToString(partition.InSyncReplicas)}");
                    });
                });
            }
        }
        public static async Task Main()
        {
          
            var bootstrapserver = "localhost:9092";
            var topicname = "Topic2";
            PrintMetadata(bootstrapserver);
            await CreateTopicAsync(bootstrapserver, topicname);
             PrintMetadata(bootstrapserver);
            Console.Write("Topic Created");
            Console.Read();

        }
        public static void Main(string[] args)
        {
            try
            {

               Producer.ProducerAsync().Wait();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Main().Wait();
        }

       
    }
}
