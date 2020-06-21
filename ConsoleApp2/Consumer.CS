using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaTutorialBasic
{
    public class Consumer
    {
        public static void Consume()
        {
            var conf = new ConsumerConfig
            {
                GroupId = "g3",
                BootstrapServers = "localhost:9092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("Topic2");

                System.Threading.CancellationTokenSource cts = new System.Threading.CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Message?.Value}' at: '{cr.TopicPartitionOffset}'.");
                            UpdateValue();
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
        }

        public static void UpdateValue()
        {
            var db = new DataAccess();
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
            db.DecrementProviderQueueCount(provider.Id, provider);
            provider = db.GetProviderById("5eed90419543c620fcfd6b76");
            Console.WriteLine($"count:{provider.ImageQueueCount}");
        }


    }
}
    