
using Amazon.S3;
using Amazon.S3.Model;

namespace LambdaFunction.Storage;

public class FileSystemStorage : IBucketStorage
{
    
    public FileSystemStorage()
    {
    }

    public Task<Stream> GetObjectStreamAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var filename = $"{GetTestDataDirectory()}/{key}";
        Stream stream = new FileStream(filename, FileMode.Open, FileAccess.Read);
        return Task.FromResult(stream);
    }

    public Task PutObjectAsync(string bucketName, string key, string filePath, CancellationToken cancellationToken = default)
    {
        var filename = $"{GetTestDataDirectory()}/{key}";
        using var readStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        using var fileStream = new FileStream(filename, FileMode.Create, FileAccess.Write);
        return readStream.CopyToAsync(fileStream, cancellationToken);
    }

    public string GetTestDataDirectory()
    {
        return "../../test/LambdaFunction.Tests/data";
    }
}