using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace LambdaFunction;

[ExcludeFromCodeCoverage]
public class Program
{
    private readonly ServiceProvider serviceProvider;

    public Program()
    {
        var services = new ServiceCollection();

        // By default, Lambda function class is added to the service container using the singleton lifetime
        // To use a different lifetime, specify the lifetime in Startup.ConfigureServices(IServiceCollection) method.
        services.AddSingleton<Function>();

        var startup = new Startup();
        startup.ConfigureServices(services);
        serviceProvider = services.BuildServiceProvider();
    }

    public async Task Run(S3Event evnt, ILambdaContext context)
    {
        // Create a scope for every request,
        // this allows creating scoped dependencies without creating a scope manually.
        using var scope = serviceProvider.CreateScope();
        var function = scope.ServiceProvider.GetRequiredService<Function>();

        await function.FunctionHandler(evnt, context);
    }

    ~Program()
    {
        serviceProvider.Dispose();
    }
}