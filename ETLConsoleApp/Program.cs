using System;
using System.Net.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using ETLConsoleApp.Services;
using ETLConsoleApp.Data;

class Program
{
    static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            })
            .ConfigureServices((context, services) =>
            {
                services.AddHttpClient();
                services.AddDbContext<ApplicationDbContext>(options =>
                    options.UseSqlServer(context.Configuration.GetConnectionString("SqlServer")));
                services.AddScoped<ETLService>();
            })
            .UseSerilog((context, config) =>
            {
                config.ReadFrom.Configuration(context.Configuration);
            })
            .Build();

        using (var scope = host.Services.CreateScope())
        {
            var services = scope.ServiceProvider;
            var etlService = services.GetRequiredService<ETLService>();
            var logger = services.GetRequiredService<ILogger<Program>>();

            try
            {
                await etlService.RunAsync();
            }
            catch (Exception ex)
            {
                logger.LogError($"Error occurred: {ex.Message}");
                logger.LogError(ex.StackTrace);
            }
        }

        await host.RunAsync();
    }
}