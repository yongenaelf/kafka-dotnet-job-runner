using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon.S3.Util;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

[Route("api")]
[ApiController]
public class BuildController : ControllerBase
{
    private readonly IAmazonS3 _s3Client;
    private readonly IConfiguration _configuration;

    public BuildController(IConfiguration configuration)
    {
        _configuration = configuration;

        var config = new AmazonS3Config
        {
            ServiceURL = _configuration.GetRequiredSection("S3:Endpoint").Value,
            ForcePathStyle = true // Use path-style addressing
        };

        _s3Client = new AmazonS3Client(_configuration.GetRequiredSection("S3:AccessKey").Value, _configuration.GetRequiredSection("S3:Secret").Value, config);
    }

    [HttpPost]
    [Route("build")]
    public async Task<IActionResult> UploadFile(IFormFile file)
    {
        if (file == null || file.Length == 0)
        {
            return BadRequest("Please upload a file.");
        }

        // Unique key for the uploaded file
        var keyName = Guid.NewGuid().ToString();

        try
        {
            var bucketName = _configuration.GetRequiredSection("S3:BucketName").Value;
            // Ensure the bucket exists
            if (!await AmazonS3Util.DoesS3BucketExistV2Async(_s3Client, bucketName))
            {
                await _s3Client.PutBucketAsync(bucketName);
            }

            // Upload the file to MinIO
            using (var newMemoryStream = new MemoryStream())
            {
                await file.CopyToAsync(newMemoryStream);
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = newMemoryStream,
                    Key = keyName,
                    BucketName = bucketName,
                    CannedACL = S3CannedACL.PublicRead
                };

                var transferUtility = new TransferUtility(_s3Client);
                await transferUtility.UploadAsync(uploadRequest);
            }
        }
        catch (Exception e)
        {
            return StatusCode(500, $"Internal server error: {e.Message}");
        }

        // produce a message to the Kafka topic
        var config = new ProducerConfig
        {
            BootstrapServers = _configuration.GetRequiredSection("Kafka:BootstrapServers").Value,
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var deliveryReport = await producer.ProduceAsync(_configuration.GetRequiredSection("Kafka:Topic").Value, new Message<Null, string> { Value = keyName });
            Console.WriteLine($"Delivered '{deliveryReport.Value}' to '{deliveryReport.TopicPartitionOffset}'");
            // wait for up to 10 seconds for any inflight messages to be delivered.
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        return Ok(new { keyName });
    }
}

