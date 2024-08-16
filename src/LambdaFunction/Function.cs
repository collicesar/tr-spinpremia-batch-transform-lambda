using System.Text.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace LambdaFunction;

public class Function
{
    IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {
        S3Client = new AmazonS3Client();
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client">The service client to access Amazon S3.</param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;
    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    /// <param name="evnt">The event for the Lambda function handler to process.</param>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var eventRecords = evnt.Records ?? new List<S3Event.S3EventNotificationRecord>();
        foreach (var record in eventRecords)
        {
            var s3Event = record.S3;
            if (s3Event == null)
            {
                continue;
            }

            try
            {
                context.Logger.LogInformation(s3Event.Object.Key);
                
                var bucket = record.S3.Bucket.Name;
                var csvKey = record.S3.Object.Key;
                var jsonKey = csvKey.Replace(".csv", ".json");

                var jsonTempFile = "/tmp/output.json";

                using (var fileStream = new FileStream(jsonTempFile, FileMode.Create, FileAccess.Write))
                using (var writer = new StreamWriter(fileStream))
                {
                    await writer.WriteAsync("[");
                    var firstLine = true;

                    await foreach (var jsonObject in ReadCsvStreaming(bucket, csvKey))
                    {
                        if (!firstLine)
                        {
                            await writer.WriteAsync(",");
                        }
                        else
                        {
                            firstLine = false;
                        }

                        var jsonLine = JsonSerializer.Serialize(jsonObject);
                        await writer.WriteAsync(jsonLine);
                    }

                    await writer.WriteAsync("]");
                }

                await S3Client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = jsonKey,
                    FilePath = jsonTempFile
                });

                File.Delete(jsonTempFile);

            }
            catch (Exception e)
            {
                context.Logger.LogError($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogError(e.Message);
                context.Logger.LogError(e.StackTrace);
                throw;
            }
        }
    }

    private async IAsyncEnumerable<Dictionary<string, string>> ReadCsvStreaming(string bucket, string key)
    {
        var request = new GetObjectRequest
        {
            BucketName = bucket,
            Key = key
        };

        using (var response = await S3Client.GetObjectAsync(request))
        using (var stream = response.ResponseStream)
        using (var reader = new StreamReader(stream))
        {
            var headerLine = await reader.ReadLineAsync();
            var headers = headerLine!.Split(',');

            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                
                if (String.IsNullOrEmpty(line))
                    continue;

                var values = line.Split(',');

                var record = new Dictionary<string, string>();
                for (int i = 0; i < headers.Length; i++)
                {
                    record[headers[i]] = values[i];
                }

                yield return record;
            }
        }
    }
}