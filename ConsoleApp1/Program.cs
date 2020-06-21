using System;
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
       
      
        public static void Main(string[] args)
        {
            try
            {

                Consumer.Consume();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

       
    }
}
