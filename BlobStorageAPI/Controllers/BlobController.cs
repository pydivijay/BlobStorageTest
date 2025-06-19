using BlobStorageTest;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace BlobStorageAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BlobController : ControllerBase
    {
        private readonly AzuriteClient _azuriteClient;

        public BlobController()
        {
            _azuriteClient = new AzuriteClient();
        }

        [HttpPost("upload")]
        public async Task<IActionResult> UploadFile(IFormFile file, string containerName)
        {
            if (file == null || file.Length == 0)
                return BadRequest("No file provided");

            await _azuriteClient.CreateContainerAsync(containerName);

            using var stream = file.OpenReadStream();
            var url = await _azuriteClient.UploadFromStreamAsync(
                containerName,
                stream,
                file.FileName
            );

            return Ok(new { Url = url, FileName = file.FileName });
        }
    }
}
