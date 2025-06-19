using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace BlobStorageAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BlobDownloadController : ControllerBase
    {
        private readonly AzuriteBlobClient _blobClient;

        public BlobDownloadController()
        {
            _blobClient = new AzuriteBlobClient();
        }

        [HttpGet("download/{containerName}/{blobName}")]
        public async Task<IActionResult> DownloadBlob(string containerName, string blobName)
        {
            try
            {
                var result = await _blobClient.DownloadBlobAsync(containerName, blobName);

                if (result == null)
                    return NotFound($"Blob '{blobName}' not found in container '{containerName}'");

                return File(result.Content, result.ContentType, result.FileName);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error downloading blob: {ex.Message}");
            }
        }

        [HttpGet("download-stream/{containerName}/{blobName}")]
        public async Task<IActionResult> DownloadBlobStream(string containerName, string blobName)
        {
            try
            {
                var stream = await _blobClient.DownloadBlobStreamAsync(containerName, blobName);

                if (stream == null)
                    return NotFound($"Blob '{blobName}' not found in container '{containerName}'");

                var contentType = await _blobClient.GetBlobContentTypeAsync(containerName, blobName);
                return File(stream, contentType ?? "application/octet-stream", blobName);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error downloading blob stream: {ex.Message}");
            }
        }

        [HttpGet("download-text/{containerName}/{blobName}")]
        public async Task<IActionResult> DownloadBlobAsText(string containerName, string blobName)
        {
            try
            {
                var content = await _blobClient.DownloadBlobAsStringAsync(containerName, blobName);

                if (content == null)
                    return NotFound($"Blob '{blobName}' not found in container '{containerName}'");

                return Ok(new { Content = content, BlobName = blobName });
            }
            catch (Exception ex)
            {
                return BadRequest($"Error downloading blob as text: {ex.Message}");
            }
        }

        [HttpGet("download-range/{containerName}/{blobName}")]
        public async Task<IActionResult> DownloadBlobRange(string containerName, string blobName, [FromQuery] long offset = 0, [FromQuery] long? count = null)
        {
            try
            {
                var result = await _blobClient.DownloadBlobRangeAsync(containerName, blobName, offset, count);

                if (result == null)
                    return NotFound($"Blob '{blobName}' not found in container '{containerName}'");

                return File(result.Content, result.ContentType, $"{blobName}_range_{offset}_{count}");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error downloading blob range: {ex.Message}");
            }
        }

        [HttpGet("info/{containerName}/{blobName}")]
        public async Task<IActionResult> GetBlobInfo(string containerName, string blobName)
        {
            try
            {
                var info = await _blobClient.GetBlobInfoAsync(containerName, blobName);

                if (info == null)
                    return NotFound($"Blob '{blobName}' not found in container '{containerName}'");

                return Ok(info);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error getting blob info: {ex.Message}");
            }
        }

        [HttpGet("list/{containerName}")]
        public async Task<IActionResult> ListBlobs(string containerName, [FromQuery] string? prefix = null)
        {
            try
            {
                var blobs = await _blobClient.ListBlobsAsync(containerName, prefix);
                return Ok(blobs);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error listing blobs: {ex.Message}");
            }
        }
    }
}
