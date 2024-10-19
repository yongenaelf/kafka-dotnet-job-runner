using Amazon.S3;
using Amazon.S3.Model;
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

        // wait for the file to be processed
        await Task.Delay(6000);

        var timeoutValue = _configuration.GetRequiredSection("Timeout").Get<int>();

        // for the next X seconds, check if the file output is in MinIO
        var timeout = DateTime.UtcNow.AddSeconds(timeoutValue);
        var exists = false;
        while (DateTime.UtcNow < timeout)
        {
            try
            {
                var metadata = await _s3Client.GetObjectMetadataAsync(_configuration.GetRequiredSection("S3:BucketName").Value, keyName + ".dll");
                if (metadata != null)
                {
                    exists = true;
                    break;
                }
            }
            catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // wait for a second before checking again
                await Task.Delay(1000);
                continue;
            }
        }

        if (!exists)
        {
            return StatusCode(500, "Operation timed out. Please try again later.");
        }

        // get the file from MinIO
        var openStreamRequest = new TransferUtilityOpenStreamRequest
        {
            Key = keyName + ".dll",
            BucketName = _configuration.GetRequiredSection("S3:BucketName").Value
        };

        // download to memorystream
        using (var transferUtility = new TransferUtility(_s3Client))
        {
            var memoryStream = new MemoryStream();
            using var responseStream = await transferUtility.OpenStreamAsync(openStreamRequest);
            await responseStream.CopyToAsync(memoryStream);
            memoryStream.Position = 0;

            // get the base64string
            var base64String = Convert.ToBase64String(memoryStream.ToArray());

            // delete the file from MinIO
            var deleteRequest = new DeleteObjectRequest
            {
                BucketName = _configuration.GetRequiredSection("S3:BucketName").Value,
                Key = keyName + ".dll"
            };

            await _s3Client.DeleteObjectAsync(deleteRequest);

            return Ok(new { dll = base64String });
        }
    }
}

