// See https://aka.ms/new-console-template for more information
using BlobStorageTest;

var client = new AzuriteClient();

// Create container
await client.CreateContainerAsync("test-container");

// Upload a file
await client.UploadFileAsync("test-container", @"C:\Users\vijay kumar pydi\Downloads\ToCollector.pdf");

// Upload string content
await client.UploadFromStringAsync("test-container", "Hello, Azurite!", "hello.txt");

// List blobs
await client.ListBlobsAsync("test-container");

// Download file
await client.DownloadFileAsync("test-container", "ToCollector.pdf", @"C:\hello1.pdf");

// Download as string
var content = await client.DownloadAsStringAsync("test-container", "hello.txt");
Console.WriteLine($"Downloaded content: {content}");

// Queue operations
var queueClient = new AzuriteQueueClient();

await queueClient.CreateQueueAsync("test-queue");
await queueClient.SendMessageAsync("test-queue", "Hello, Queue!");

var messages = await queueClient.ReceiveMessagesAsync("test-queue");
foreach (var message in messages)
{
    Console.WriteLine($"Received: {message.MessageText}");
    await queueClient.DeleteMessageAsync("test-queue", message.MessageId, message.PopReceipt);
}

// Table operations
var tableClient = new AzuriteTableClient();

await tableClient.CreateTableAsync("customers");

var customer = new CustomerEntity
{
    PartitionKey = "CustomerType1",
    RowKey = Guid.NewGuid().ToString(),
    Name = "John Doe",
    Email = "john@example.com",
    Age = 30,
    CreatedDate = DateTime.UtcNow
};

await tableClient.InsertEntityAsync("customers", customer);

var retrievedCustomer = await tableClient.GetEntityAsync<CustomerEntity>("customers", customer.PartitionKey, customer.RowKey);
Console.WriteLine($"Retrieved customer: {retrievedCustomer?.Name}");

var allCustomers = await tableClient.QueryEntitiesAsync<CustomerEntity>("customers");
Console.WriteLine($"Total customers: {allCustomers.Count}");