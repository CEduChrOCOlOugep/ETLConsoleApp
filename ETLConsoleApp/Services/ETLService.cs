using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using ETLConsoleApp.Models;
using ETLConsoleApp.Data;

namespace ETLConsoleApp.Services
{
    public class ETLService
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiBaseUrl;
        private readonly string _encodedCredentials;
        private readonly ILogger<ETLService> _logger;
        private readonly ApplicationDbContext _dbContext;

        public ETLService(HttpClient httpClient, IConfiguration configuration, ILogger<ETLService> logger, ApplicationDbContext dbContext)
        {
            _httpClient = httpClient;
            _apiBaseUrl = configuration["ApiSettings:BaseUrl"];
            var username = configuration["ApiSettings:Username"];
            var password = configuration["ApiSettings:Password"];
            _encodedCredentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{username}:{password}"));
            _logger = logger;
            _dbContext = dbContext;
        }

        private Dictionary<string, object> CreatePayload(string appid, int pageNumber = 1, int pageSize = 5000, string fieldname = "LAST_UPDATE", string lastUpdate = "20240501000000")
        {
            return new Dictionary<string, object>
            {
                { "appid", appid },
                { "pagenumber", pageNumber },
                { "pagesize", pageSize },
                { "filter", new Dictionary<string, object>
                    {
                        { "query", new List<Dictionary<string, string>>
                            {
                                new Dictionary<string, string>
                                {
                                    { "fieldname", fieldname },
                                    { "operand", ">=" },
                                    { "fieldvalue", lastUpdate }
                                }
                            }
                        }
                    }
                }
            };
        }

        private async Task<List<Payload1Response>> FetchData1Async()
        {
            var allData = new List<Payload1Response>();
            var page = 1;
            var pageSize = 5000;

            while (true)
            {
                try
                {
                    var data = CreatePayload("payload1_appid", page, pageSize);
                    var jsonPayload = JsonConvert.SerializeObject(data);
                    var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                    requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

                    var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);
                    response.EnsureSuccessStatusCode();

                    var responseData = JsonConvert.DeserializeObject<ApiResponse>(await response.Content.ReadAsStringAsync());
                    foreach (var row in responseData.Row)
                    {
                        var item = new Payload1Response();
                        foreach (var field in row.Field)
                        {
                            if (field.Name == "PERN")
                                item.PERN = field.Value;
                            else if (field.Name == "Field1")
                                item.Field1 = field.Value;
                            // Add other fields as necessary
                        }
                        allData.Add(item);
                    }

                    if (page * pageSize >= int.Parse(responseData.TotalRecordCount))
                        break;

                    page++;
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error occurred while fetching data1, page {page}: {ex.Message}");
                    return null;
                }
            }

            return allData;
        }

        private async Task<List<Payload2Response>> FetchData2Async()
        {
            var allData = new List<Payload2Response>();
            var page = 1;
            var pageSize = 5000;

            while (true)
            {
                try
                {
                    var data = CreatePayload("payload2_appid", page, pageSize);
                    var jsonPayload = JsonConvert.SerializeObject(data);
                    var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                    requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

                    var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);
                    response.EnsureSuccessStatusCode();

                    var responseData = JsonConvert.DeserializeObject<ApiResponse>(await response.Content.ReadAsStringAsync());
                    foreach (var row in responseData.Row)
                    {
                        var item = new Payload2Response();
                        foreach (var field in row.Field)
                        {
                            if (field.Name == "PERN")
                                item.PERN = field.Value;
                            else if (field.Name == "Field2")
                                item.Field2 = field.Value;
                            // Add other fields as necessary
                        }
                        allData.Add(item);
                    }

                    if (page * pageSize >= int.Parse(responseData.TotalRecordCount))
                        break;

                    page++;
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error occurred while fetching data2, page {page}: {ex.Message}");
                    return null;
                }
            }

            return allData;
        }

        public async Task RunAsync()
        {
            var data1 = await FetchData1Async();
            var data2 = await FetchData2Async();

            if (data1 == null || data2 == null)
            {
                _logger.LogError("Failed to fetch data");
                return;
            }

            var joinedData = from d1 in data1
                             join d2 in data2 on d1.PERN equals d2.PERN
                             select new DataIntegration
                             {
                                 PERN = d1.PERN,
                                 Field1 = d1.Field1,
                                 Field2 = d2.Field2
                                 // Add other fields as necessary
                             };

            foreach (var record in joinedData)
            {
                var existingRecord = await _dbContext.DataIntegration.FindAsync(record.PERN);
                if (existingRecord == null)
                {
                    _dbContext.DataIntegration.Add(record);
                }
                else
                {
                    existingRecord.Field1 = record.Field1;
                    existingRecord.Field2 = record.Field2;
                    // Update other fields as necessary
                }
            }

            await _dbContext.SaveChangesAsync();
        }
    }
}
