using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using ETLConsoleApp.Services;
using ETLConsoleApp.Data;
using Microsoft.EntityFrameworkCore;

namespace ETLConsoleApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Configure Serilog
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            try
            {
                var host = Host.CreateDefaultBuilder(args)
                    .ConfigureAppConfiguration((hostingContext, config) =>
                    {
                        config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
                    })
                    .ConfigureServices((context, services) =>
                    {
                        // Add services to the container.
                        services.AddHttpClient();
                        services.AddDbContext<ApplicationDbContext>(options =>
                            options.UseSqlServer(context.Configuration.GetConnectionString("SqlServer")));
                        services.AddScoped<ETLService>();

                        // Use Serilog for logging
                        services.AddLogging(loggingBuilder =>
                            loggingBuilder.AddSerilog(dispose: true));
                    })
                    .Build();

                using (var scope = host.Services.CreateScope())
                {
                    var services = scope.ServiceProvider;
                    var etlService = services.GetRequiredService<ETLService>();
                    var logger = services.GetRequiredService<ILogger<ETLService>>();

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
            catch (Exception ex)
            {
                Log.Fatal(ex, "Host terminated unexpectedly");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }
    }
}