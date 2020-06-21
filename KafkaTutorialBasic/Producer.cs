using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaTutorialBasic
{
    public class Producer
    {
        public static DataAccess db = new DataAccess();
        public static async Task ProducerAsync()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.Leader,
                BatchNumMessages = 10,
                LingerMs = 1,
                MessageTimeoutMs = 5000,
                Partitioner = Partitioner.ConsistentRandom,
                MessageSendMaxRetries = 3


            };
            TopicPartition tp = new TopicPartition("Topic2", 3);


            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            { 
                ResetCount();
                try
                {
                    for (int i = 1; i <= 5000; i++)
                    {


                        var dr = await p.ProduceAsync("Topic2", new Message<Null, string> { Value = "test" + i });

                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                        UpdateValue();
                    }


                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }



        }
        public static void UpdateValue()
        {
            var list = db.GetAll();
            Provider provider;
            if (list.Count() == 0)
            {
                provider = new Provider() { ImageQueueCount = 0 };
                provider = db.Create(provider);
            }
            else
            {
                provider = db.GetProviderById("5eed90419543c620fcfd6b76");
            }
            db.IncrementProviderImageResizeQueueCount(provider.Id, provider);
            provider = db.GetProviderById("5eed90419543c620fcfd6b76");
            Console.WriteLine($"count:{provider.ImageQueueCount}");
        }

        public static void ResetCount()
        {
            var provider = db.GetProviderById("5eed90419543c620fcfd6b76");
            if (provider.ImageQueueCount < 0)
            {
                provider.ImageQueueCount = 0;
                db.Update(provider);
                provider = db.GetProviderById("5eed90419543c620fcfd6b76");
                Console.WriteLine($"count:{provider.ImageQueueCount}");
            }
        }
    }
}
