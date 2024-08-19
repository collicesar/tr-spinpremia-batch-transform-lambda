using System.Text.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3.Model;
using LambdaFunction.Storage;
using Datadog.Trace.Annotations;

namespace LambdaFunction;

public class Function
{
    private readonly IBucketStorage bucketStorage;
    private readonly string inputPath = "inputs";
    private readonly string outputPath = "outputs";

    // <summary>
    /// Constructs an instance with a preconfigured Bucket Storage. This can be used for testing outside of the Lambda environment.
    /// </summary>
    /// <param name="bucketStorage">The bucket storage manager</param>
    public Function(IBucketStorage bucketStorage)
    {
        this.bucketStorage = bucketStorage;
    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    /// <param name="evnt">The event for the Lambda function handler to process.</param>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    [Trace(OperationName = "TransformFunction.FunctionHandler", ResourceName = "TransformFunction")] 
    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var eventRecords = evnt.Records ?? new List<S3Event.S3EventNotificationRecord>();

        context.Logger.LogInformation($"Start to proccesing CSV files");
        await foreach (var jsonResult in TransformCsvToJson(eventRecords))
        {
            var csvKey  = jsonResult["csvKey"];
            var jsonTempFile  = jsonResult["jsonTempFile"];
            var bucket  = jsonResult["bucket"];

            if ( jsonResult.ContainsKey("error") )
            {
                var errorMessage  = jsonResult["error"];
                context.Logger.LogError($"CSV {csvKey} with error {errorMessage}");
                continue;
            }
            context.Logger.LogInformation($"Json generated: {jsonTempFile}");

            var jsonKey = csvKey.Replace(".csv", ".json").Replace(inputPath, outputPath);

            try
            {
                await bucketStorage.PutObjectAsync(bucket, jsonKey, jsonTempFile);
                context.Logger.LogInformation($"Json uploaded: {jsonKey}");
            }
            catch (Exception ex)
            {
                context.Logger.LogError($"Uploading file {jsonKey} with error: {ex.Message}");
            }
            finally
            {
                File.Delete(jsonTempFile);
                context.Logger.LogInformation($"Temp file deleted: {jsonTempFile}");
            }
        }
        context.Logger.LogInformation($"End to proccesing CSV files");
    }

    private async IAsyncEnumerable<Dictionary<string, string>> TransformCsvToJson(List<S3Event.S3EventNotificationRecord> records)
    {
        foreach (var record in records)
        {
            if (!record.S3.Object.Key.EndsWith(".csv"))
                continue;

            Guid uuid = Guid.NewGuid();
            var bucket  = record.S3.Bucket.Name;
            var csvKey  = record.S3.Object.Key;
            var jsonKey = csvKey.Replace(".csv", ".json").Replace(inputPath, outputPath);
            var jsonTempFile = $"/tmp/{uuid}.json";

            var item = new Dictionary<string, string>(){
                { "bucket", bucket },
                { "csvKey", csvKey },
                { "jsonTempFile", jsonTempFile },
            };

            try
            {
                using (var fileStream = new FileStream(jsonTempFile, FileMode.Create, FileAccess.Write))
                using (var writer = new StreamWriter(fileStream))
                {
                    await foreach (var jsonObject in ReadCsvStreaming(bucket, csvKey))
                    {
                        var jsonLine = JsonSerializer.Serialize(jsonObject);
                        await writer.WriteLineAsync(jsonLine);
                    }
                }                
            }
            catch (System.Exception ex)
            {
                item["error"] = ex.Message;
            }

            yield return item;
        }
    }

    private async IAsyncEnumerable<Dictionary<string, string>> ReadCsvStreaming(string bucket, string key)
    {
        var request = new GetObjectRequest
        {
            BucketName = bucket,
            Key = key
        };

        using (var stream = await bucketStorage.GetObjectStreamAsync(bucket, key))
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