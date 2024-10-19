using Amazon.S3;
using Amazon.S3.Transfer;
using Confluent.Kafka;
using System.Diagnostics;
using System.IO.Compression;
using Microsoft.Extensions.Configuration;
using Amazon.S3.Model;

// load from appsettings.json
var config = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .Build();

var s3Config = new AmazonS3Config
{
    ServiceURL = config.GetRequiredSection("S3:Endpoint").Value,
    ForcePathStyle = true // Use path-style addressing
};

var _s3Client = new AmazonS3Client(config.GetRequiredSection("S3:AccessKey").Value, config.GetRequiredSection("S3:Secret").Value, s3Config);

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = config.GetRequiredSection("Kafka:BootstrapServers").Value,
    GroupId = "build-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Latest
};

using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
{
    consumer.Subscribe("build");

    CancellationTokenSource cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true; // prevent the process from terminating.
        cts.Cancel();
    };

    var filePath = "";
    var extractPath = "";

    try
    {
        var consumeResult = consumer.Consume(cts.Token);
        Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' from topic '{consumeResult.Topic}', partition '{consumeResult.Partition}', offset '{consumeResult.Offset}'");

        // download file from MinIO
        var bucketName = config.GetRequiredSection("S3:BucketName").Value;
        var keyName = consumeResult.Message.Value;
        filePath = Path.Combine(Path.GetTempPath(), keyName + ".zip");

        var downloadRequest = new TransferUtilityDownloadRequest
        {
            Key = keyName,
            BucketName = bucketName,
            FilePath = filePath
        };

        using (var transferUtility = new TransferUtility(_s3Client))
        {
            await transferUtility.DownloadAsync(downloadRequest);
        }

        Console.WriteLine($"Downloaded file to '{filePath}'");

        // delete from MinIO
        var deleteRequest = new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = keyName
        };

        await _s3Client.DeleteObjectAsync(deleteRequest);
        Console.WriteLine($"Deleted file '{keyName}' from bucket '{bucketName}'");

        // extract the file
        var zipPath = filePath;
        extractPath = Path.Combine(Path.GetTempPath(), keyName, "extracted");

        ZipFile.ExtractToDirectory(zipPath, extractPath);
        Console.WriteLine($"Extracted file to '{extractPath}'");
        // log the file in a tree
        var files = Directory.GetFiles(extractPath, "*.*", SearchOption.AllDirectories);
        foreach (var file in files)
        {
            Console.WriteLine(file);
        }

        var projectFiles = Directory.GetFiles(extractPath, "*.csproj", SearchOption.AllDirectories);
        Console.WriteLine($"Found {projectFiles.Length} project files");
        var projectFile = projectFiles.FirstOrDefault();

        if (projectFile == null)
        {
            Console.WriteLine("No project file found");
            return;
        }

        // build the project
        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = $"build {projectFile}",
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            }
        };

        process.Start();
        process.WaitForExit();

        var output = await process.StandardOutput.ReadToEndAsync();
        Console.WriteLine($"Build output: {output}");

        Console.WriteLine($"Built project '{projectFile}'");

        // get the content of the first .dll file base64 string
        var directory = Path.GetDirectoryName(extractPath);

        if (directory == null)
        {
            Console.WriteLine("No directory found");
            return;
        }

        var dllFiles = Directory.GetFiles(directory, "*.dll.patched", SearchOption.AllDirectories);
        var dllFile = dllFiles.FirstOrDefault();
        if (dllFile == null)
        {
            Console.WriteLine("No DLL file found");
            return;
        }

        // save the file to MinIO
        var uploadRequest = new TransferUtilityUploadRequest
        {
            FilePath = dllFile,
            Key = keyName + ".dll",
            BucketName = bucketName,
            CannedACL = S3CannedACL.PublicRead
        };

        using (var transferUtility = new TransferUtility(_s3Client))
        {
            await transferUtility.UploadAsync(uploadRequest);
        }

        Console.WriteLine($"Uploaded file '{dllFile}' to bucket '{bucketName}' with key '{keyName}.dll'");
    }
    catch (ConsumeException e)
    {
        Console.WriteLine($"Error occurred: {e.Error.Reason}");
    }
    finally
    {
        consumer.Close();

        if (File.Exists(filePath))
        {
            File.Delete(filePath);
            Console.WriteLine($"Deleted file '{filePath}'");
        }

        if (Directory.Exists(extractPath))
        {
            // find the parent directory
            var parentDirectory = Path.GetDirectoryName(extractPath);
            if (parentDirectory != null)
            {
                // delete the parent directory
                Directory.Delete(parentDirectory, true);
                Console.WriteLine($"Deleted directory '{parentDirectory}'");
            }
        }
    }
}
