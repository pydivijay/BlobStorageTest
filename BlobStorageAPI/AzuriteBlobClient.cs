using Azure.Storage.Blobs;
using Azure;
using System.Text;

namespace BlobStorageAPI
{
    // Enhanced Blob Client with Download Methods
    public class AzuriteBlobClient
    {
        private readonly BlobServiceClient _blobServiceClient;
        private const string ConnectionString =
            "DefaultEndpointsProtocol=http;" +
            "AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
            "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

        public AzuriteBlobClient()
        {
            _blobServiceClient = new BlobServiceClient(ConnectionString);
        }

        public async Task<BlobDownloadResult?> DownloadBlobAsync(string containerName, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    return null;

                var response = await blobClient.DownloadContentAsync();
                var content = response.Value.Content.ToArray();
                var contentType = response.Value.Details.ContentType;

                return new BlobDownloadResult
                {
                    Content = content,
                    ContentType = contentType,
                    FileName = blobName,
                    Size = content.Length,
                    LastModified = response.Value.Details.LastModified,
                    ETag = response.Value.Details.ETag.ToString()
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading blob: {ex.Message}");
                return null;
            }
        }

        public async Task<Stream?> DownloadBlobStreamAsync(string containerName, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    return null;

                var response = await blobClient.DownloadStreamingAsync();
                return response.Value.Content;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading blob stream: {ex.Message}");
                return null;
            }
        }

        public async Task<string?> DownloadBlobAsStringAsync(string containerName, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    return null;

                var response = await blobClient.DownloadContentAsync();
                return response.Value.Content.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading blob as string: {ex.Message}");
                return null;
            }
        }

        public async Task<BlobDownloadResult?> DownloadBlobRangeAsync(string containerName, string blobName, long offset, long? count = null)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    return null;

                var range = new HttpRange(offset, count);
                var response = await blobClient.DownloadContentAsync();
                var content = response.Value.Content.ToArray();

                return new BlobDownloadResult
                {
                    Content = content,
                    ContentType = response.Value.Details.ContentType,
                    FileName = blobName,
                    Size = content.Length,
                    LastModified = response.Value.Details.LastModified,
                    ETag = response.Value.Details.ETag.ToString()
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading blob range: {ex.Message}");
                return null;
            }
        }

        public async Task<bool> DownloadBlobToFileAsync(string containerName, string blobName, string localFilePath)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    return false;

                using var fileStream = File.Create(localFilePath);
                await blobClient.DownloadToAsync(fileStream);

                Console.WriteLine($"Blob '{blobName}' downloaded to '{localFilePath}'");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error downloading blob to file: {ex.Message}");
                return false;
            }
        }

        public async Task<string?> GetBlobContentTypeAsync(string containerName, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                var properties = await blobClient.GetPropertiesAsync();
                return properties.Value.ContentType;
            }
            catch
            {
                return "application/octet-stream";
            }
        }

        public async Task<BlobInfo?> GetBlobInfoAsync(string containerName, string blobName)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobClient = containerClient.GetBlobClient(blobName);

                if (!await blobClient.ExistsAsync())
                    return null;

                var properties = await blobClient.GetPropertiesAsync();
                var props = properties.Value;

                return new BlobInfo
                {
                    Name = blobName,
                    ContainerName = containerName,
                    Size = props.ContentLength,
                    ContentType = props.ContentType,
                    LastModified = props.LastModified,
                    ETag = props.ETag.ToString(),
                    Url = blobClient.Uri.ToString(),
                    Metadata = props.Metadata
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting blob info: {ex.Message}");
                return null;
            }
        }

        public async Task<List<BlobInfo>> ListBlobsAsync(string containerName, string? prefix = null)
        {
            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
                var blobs = new List<BlobInfo>();

                await foreach (var blob in containerClient.GetBlobsAsync(prefix: prefix))
                {
                    blobs.Add(new BlobInfo
                    {
                        Name = blob.Name,
                        ContainerName = containerName,
                        Size = blob.Properties.ContentLength ?? 0,
                        ContentType = blob.Properties.ContentType,
                        LastModified = blob.Properties.LastModified,
                        ETag = blob.Properties.ETag?.ToString(),
                        Url = containerClient.GetBlobClient(blob.Name).Uri.ToString()
                    });
                }

                return blobs;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error listing blobs: {ex.Message}");
                return new List<BlobInfo>();
            }
        }
    }

    // Data Models
    public class BlobDownloadResult
    {
        public byte[] Content { get; set; } = Array.Empty<byte>();
        public string ContentType { get; set; } = string.Empty;
        public string FileName { get; set; } = string.Empty;
        public long Size { get; set; }
        public DateTimeOffset? LastModified { get; set; }
        public string? ETag { get; set; }
    }

    public class BlobInfo
    {
        public string Name { get; set; } = string.Empty;
        public string ContainerName { get; set; } = string.Empty;
        public long Size { get; set; }
        public string? ContentType { get; set; }
        public DateTimeOffset? LastModified { get; set; }
        public string? ETag { get; set; }
        public string Url { get; set; } = string.Empty;
        public IDictionary<string, string>? Metadata { get; set; }
    }
}
