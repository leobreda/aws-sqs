using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ConsoleSQS
{
    class Program
    {
        private static string URL_SQS = "https://url-sqs-aqui";
        private static int TOTAL_MENSAGENS = 100;

        private static int MENSAGENS_LEITURA = 10;
        private static int SEGUNDOS_QUEUE = 20;

        public static void Main(string[] args)
        {
            Program instancia = new Program();
            AmazonSQSClient client = new AmazonSQSClient();
            instancia.Inicializar(client);


        }

        public void Inicializar(AmazonSQSClient client)
        {
            Console.WriteLine("Selecione a opção abaixo:");
            Console.WriteLine($"1 para produzir {TOTAL_MENSAGENS} mensagens");
            Console.WriteLine($"2 para produzir 1 mensagem com 240kb");

            Console.WriteLine("3 para consumir mensagens");
            int opcao = 0;
            try
            {
                opcao = System.Convert.ToInt32(Console.ReadLine().ToString());
            }
            catch{opcao = 0;}

            switch(opcao)
            {
                case 1:
                    this.ProduzirMensagem(client).Wait();
                    break;

                case 2:
                    this.ProduzirMensagem240kb(client).Wait();
                    break;

                case 3:
                    this.ConsumirMensagem(client).Wait();
                    break;

                default:
                    break;
            }

            System.Threading.Thread.Sleep(100);
            Console.Clear();
            this.Inicializar(client);
        }

        private async Task ProduzirMensagem(AmazonSQSClient client)
        {
            Console.WriteLine("Produzindo mensagens...");
            System.Threading.Thread.Sleep(1000);

            for (int i = 0; i < TOTAL_MENSAGENS; i++)
            {
                await client.SendMessageAsync(URL_SQS, $"Mensagem {i.ToString().PadLeft(4, '0')}: Lorem ipsum samet dolor");
                Console.Write(".");
            }
            Console.WriteLine("");
        }


        private async Task ProduzirMensagem240kb(AmazonSQSClient client)
        {
            Console.WriteLine("Produzindo mensagem com 240kb...");
            System.Threading.Thread.Sleep(1000);

            await client.SendMessageAsync(URL_SQS, LoremIpsum.text240kb);
            
            
        }

        private async Task ConsumirMensagem(AmazonSQSClient client)
        {
            Console.WriteLine("Consumindo mensagens...");
            System.Threading.Thread.Sleep(1000);


            Console.WriteLine("Pressione qualquer tecla pra interromper a leitura (aguarde a desconexão do QUEUE).");
            do
            {
                var msg = await this.GetMessages(client);
                if (msg.Messages.Count != 0)
                {
                    for (int i = 0; i < msg.Messages.Count; i++)
                    {
                        Console.WriteLine($"\nMensagem recuperada: {msg.Messages[i].Body}");

                        client.DeleteMessageAsync(URL_SQS, msg.Messages[i].ReceiptHandle).Wait();
                        Console.Write(".");
                    }
                    Console.WriteLine("");

                }
            } while (!Console.KeyAvailable);
        }

        private async Task<ReceiveMessageResponse> GetMessages(IAmazonSQS sqsClient)
        {
            Console.WriteLine("Recebendo mensagens...");
            return await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = URL_SQS,
                MaxNumberOfMessages = MENSAGENS_LEITURA,
                WaitTimeSeconds = SEGUNDOS_QUEUE
            });
        }
    }
}
