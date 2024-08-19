namespace LambdaFunction.Storage;

public interface IBucketStorage
{
    Task<Stream> GetObjectStreamAsync(string bucketName, string key, CancellationToken cancellationToken = default(CancellationToken));
    Task PutObjectAsync(string bucketName, string key, string filePath, CancellationToken cancellationToken = default(CancellationToken));
}