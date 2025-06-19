using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobStorageTest
{
    public class AzuriteClient
    {
        private readonly BlobServiceClient _blobServiceClient;
        private const string ConnectionString =
            "DefaultEndpointsProtocol=http;" +
            "AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
            "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

        public AzuriteClient()
        {
            _blobServiceClient = new BlobServiceClient(ConnectionString);
        }

        public async Task<BlobContainerClient> CreateContainerAsync(string containerName)
        {
            try
            {
                var containerClient = await _blobServiceClient.CreateBlobContainerAsync(containerName);
                Console.WriteLine($"Container '{containerName}' created successfully");
                return containerClient;
            }
            catch (Exception ex) when (ex.Message.Contains("ContainerAlreadyExists"))
            {
                Console.WriteLine($"Container '{containerName}' already exists");
                return _blobServiceClient.GetBlobContainerClient(containerName);
            }
        }

        public async Task<string?> UploadFileAsync(string containerName, string filePath, string? blobName = null)
        {
            blobName ??= Path.GetFileName(filePath);

            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                using var fileStream = File.OpenRead(filePath);
                await blobClient.UploadAsync(fileStream, overwrite: true);

                Console.WriteLine($"File '{filePath}' uploaded as '{blobName}' to container '{containerName}'");
                return blobClient.Uri.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading file: {ex.Message}");
                return null;
            }
        }

        public async Task<string?> UploadFromStringAsync(string containerName, string content, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
                await blobClient.UploadAsync(stream, overwrite: true);

                Console.WriteLine($"Content uploaded as '{blobName}' to container '{containerName}'");
                return blobClient.Uri.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading content: {ex.Message}");
                return null;
            }
        }

        public async Task<string?> UploadFromStreamAsync(string containerName, Stream stream, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                stream.Position = 0; // Reset stream position
                await blobClient.UploadAsync(stream, overwrite: true);

                Console.WriteLine($"Stream uploaded as '{blobName}' to container '{containerName}'");
                return blobClient.Uri.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading stream: {ex.Message}");
                return null;
            }
        }

        public async Task ListBlobsAsync(string containerName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);

                Console.WriteLine($"Blobs in container '{containerName}':");
                await foreach (var blob in containerClient.GetBlobsAsync())
                {
                    Console.WriteLine($"  - {blob.Name} (Size: {blob.Properties.ContentLength} bytes)");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error listing blobs: {ex.Message}");
            }
        }

        public async Task<bool> DownloadFileAsync(string containerName, string blobName, string downloadPath)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                using var fileStream = File.Create(downloadPath);
                await blobClient.DownloadToAsync(fileStream);

                Console.WriteLine($"Blob '{blobName}' downloaded to '{downloadPath}'");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading file: {ex.Message}");
                return false;
            }
        }

        public async Task<string?> DownloadAsStringAsync(string containerName, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                var response = await blobClient.DownloadContentAsync();
                return response.Value.Content.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading content: {ex.Message}");
                return null;
            }
        }

        public async Task<bool> DeleteBlobAsync(string containerName, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);
                await blobClient.DeleteIfExistsAsync();

                Console.WriteLine($"Blob '{blobName}' deleted from container '{containerName}'");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting blob: {ex.Message}");
                return false;
            }
        }
    }
}