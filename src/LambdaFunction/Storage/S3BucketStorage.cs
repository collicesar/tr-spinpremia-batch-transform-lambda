
using Amazon.S3;
using Amazon.S3.Model;

namespace LambdaFunction.Storage;

public class S3BucketStorage : IBucketStorage
{
    private IAmazonS3 _s3client;

    public S3BucketStorage(IAmazonS3 s3Client)
    {
        _s3client = s3Client;
    }

    public Task<Stream> GetObjectStreamAsync(string bucketName, string key, CancellationToken cancellationToken = default)
    {
        var request = new GetObjectRequest
        {
            BucketName = bucketName,
            Key = key
        };

        using var response = _s3client.GetObjectAsync(request, cancellationToken);
        return Task.FromResult(response.Result.ResponseStream);
    }

    public Task PutObjectAsync(string bucketName, string key, string filePath, CancellationToken cancellationToken = default)
    {
        return _s3client.PutObjectAsync(
            new PutObjectRequest
            {
                BucketName = bucketName,
                Key = key,
                FilePath = filePath
            },
            cancellationToken
        );
    }
}