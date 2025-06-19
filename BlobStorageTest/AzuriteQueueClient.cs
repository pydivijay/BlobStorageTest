using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace BlobStorageTest
{
    // Queue Storage Client
    public class AzuriteQueueClient
    {
        private readonly QueueServiceClient _queueServiceClient;
        private const string ConnectionString =
            "DefaultEndpointsProtocol=http;" +
            "AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
            "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;";

        public AzuriteQueueClient()
        {
            _queueServiceClient = new QueueServiceClient(ConnectionString);
        }

        public async Task<QueueClient> CreateQueueAsync(string queueName)
        {
            try
            {
                var queueClient = await _queueServiceClient.CreateQueueAsync(queueName);
                Console.WriteLine($"Queue '{queueName}' created successfully");
                return queueClient;
            }
            catch (Exception ex) when (ex.Message.Contains("QueueAlreadyExists"))
            {
                Console.WriteLine($"Queue '{queueName}' already exists");
                return _queueServiceClient.GetQueueClient(queueName);
            }
        }

        public async Task<bool> SendMessageAsync(string queueName, string message)
        {
            try
            {
                var queueClient = _queueServiceClient.GetQueueClient(queueName);
                await queueClient.SendMessageAsync(message);

                Console.WriteLine($"Message sent to queue '{queueName}': {message}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending message: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> SendJsonMessageAsync<T>(string queueName, T messageObject)
        {
            try
            {
                var json = JsonSerializer.Serialize(messageObject);
                return await SendMessageAsync(queueName, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error sending JSON message: {ex.Message}");
                return false;
            }
        }

        public async Task<QueueMessage[]> ReceiveMessagesAsync(string queueName, int maxMessages = 1)
        {
            try
            {
                var queueClient = _queueServiceClient.GetQueueClient(queueName);
                var response = await queueClient.ReceiveMessagesAsync(maxMessages);

                Console.WriteLine($"Received {response.Value.Length} messages from queue '{queueName}'");
                return response.Value;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error receiving messages: {ex.Message}");
                return Array.Empty<QueueMessage>();
            }
        }

        public async Task<T?> ReceiveJsonMessageAsync<T>(string queueName)
        {
            try
            {
                var messages = await ReceiveMessagesAsync(queueName, 1);
                if (messages.Length > 0)
                {
                    var json = messages[0].MessageText;
                    return JsonSerializer.Deserialize<T>(json);
                }
                return default(T);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error receiving JSON message: {ex.Message}");
                return default(T);
            }
        }

        public async Task<bool> DeleteMessageAsync(string queueName, string messageId, string popReceipt)
        {
            try
            {
                var queueClient = _queueServiceClient.GetQueueClient(queueName);
                await queueClient.DeleteMessageAsync(messageId, popReceipt);

                Console.WriteLine($"Message deleted from queue '{queueName}'");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting message: {ex.Message}");
                return false;
            }
        }

        public async Task<QueueProperties?> GetQueuePropertiesAsync(string queueName)
        {
            try
            {
                var queueClient = _queueServiceClient.GetQueueClient(queueName);
                var response = await queueClient.GetPropertiesAsync();

                Console.WriteLine($"Queue '{queueName}' has {response.Value.ApproximateMessagesCount} messages");
                return response.Value;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting queue properties: {ex.Message}");
                return null;
            }
        }

        public async Task<bool> ClearQueueAsync(string queueName)
        {
            try
            {
                var queueClient = _queueServiceClient.GetQueueClient(queueName);
                await queueClient.ClearMessagesAsync();

                Console.WriteLine($"Queue '{queueName}' cleared");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error clearing queue: {ex.Message}");
                return false;
            }
        }
    }

}
