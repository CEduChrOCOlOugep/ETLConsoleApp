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
    /// <summary>
    /// Service for Extract, Transform, and Load (ETL) operations.
    /// </summary>
    public class ETLService
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiBaseUrl;
        private readonly string _encodedCredentials;
        private readonly ILogger<ETLService> _logger;
        private readonly ApplicationDbContext _dbContext;
        private readonly string _payload1AppId;
        private readonly string _payload2AppId;

        /// <summary>
        /// Initializes a new instance of the <see cref="ETLService"/> class.
        /// </summary>
        /// <param name="httpClient">The HTTP client used for API requests.</param>
        /// <param name="configuration">The application configuration.</param>
        /// <param name="logger">The logger instance.</param>
        /// <param name="dbContext">The database context.</param>
        public ETLService(HttpClient httpClient, IConfiguration configuration, ILogger<ETLService> logger, ApplicationDbContext dbContext)
        {
            _httpClient = httpClient;
            _apiBaseUrl = configuration["ApiSettings:BaseUrl"];
            var username = configuration["ApiSettings:Username"];
            var password = configuration["ApiSettings:Password"];
            _encodedCredentials = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{username}:{password}"));
            _logger = logger;
            _dbContext = dbContext;
            _payload1AppId = configuration["ApiSettings:AppIds:Payload1AppId"];
            _payload2AppId = configuration["ApiSettings:AppIds:Payload2AppId"];
        }

        /// <summary>
        /// Creates the payload for the API request.
        /// </summary>
        /// <param name="appid">The application ID.</param>
        /// <param name="pageNumber">The page number.</param>
        /// <param name="pageSize">The page size.</param>
        /// <param name="fieldname">The field name for filtering.</param>
        /// <param name="operand">The operand for filtering.</param>
        /// <param name="fieldvalue">The field value for filtering.</param>
        /// <returns>A dictionary representing the payload.</returns>
        private Dictionary<string, object> CreatePayload(string appid, int pageNumber, int pageSize, string fieldname, string operand, string fieldvalue)
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
                                    { "operand", operand },
                                    { "fieldvalue", fieldvalue }
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
            var data = CreatePayload(_payload1AppId, page, pageSize, "LAST_UPDATE", ">=", "20240501000000");
            var jsonPayload = JsonConvert.SerializeObject(data);
            var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

            var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);
            response.EnsureSuccessStatusCode();

            var responseData = JsonConvert.DeserializeObject<ApiResponse>(await response.Content.ReadAsStringAsync());
            if (responseData.Status != "S")
            {
                _logger.LogError($"Error in response status: {responseData.Status}");
                break;
            }

            if (responseData.Row != null)
            {
                foreach (var row in responseData.Row)
                {
                    var item = new Payload1Response();
                    foreach (var field in row.Field)
                    {
                        if (field.Name == Name.PERNR)
                            item.PERN = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.FIELD1)
                            item.Field1 = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.LAST_UPDATE)
                            item.LastUpdate = field.Value.String != null ? DateTime.Parse(field.Value.String) : (DateTime?)null;
                        // Add other fields as necessary
                    }
                    allData.Add(item);
                }
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
            var data = CreatePayload(_payload2AppId, page, pageSize, "ESC", "=", "S");
            var jsonPayload = JsonConvert.SerializeObject(data);
            var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

            var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);
            response.EnsureSuccessStatusCode();

            var responseData = JsonConvert.DeserializeObject<ApiResponse>(await response.Content.ReadAsStringAsync());
            if (responseData.Status != "S")
            {
                _logger.LogError($"Error in response status: {responseData.Status}");
                break;
            }

            if (responseData.Row != null)
            {
                foreach (var row in responseData.Row)
                {
                    var item = new Payload2Response();
                    foreach (var field in row.Field)
                    {
                        if (field.Name == Name.PERNR)
                            item.PERN = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.FIELD2)
                            item.Field2 = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.LAST_UPDATE)
                            item.LastUpdate = field.Value.String != null ? DateTime.Parse(field.Value.String) : (DateTime?)null;
                        // Add other fields as necessary
                    }
                    allData.Add(item);
                }
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
        /// <summary>
        /// Fetches data for the first payload.
        /// </summary>
        /// <returns>A list of <see cref="Payload1Response"/> objects.</returns>
        private async Task<List<Payload1Response>> FetchData1Async()
        {
            var allData = new List<Payload1Response>();
            var page = 1;
            var pageSize = 5000;

            while (true)
            {
                try
                {
                    var data = CreatePayload(_payload1AppId, page, pageSize, "LAST_UPDATE", ">=", "20240501000000");
                    var jsonPayload = JsonConvert.SerializeObject(data);
                    var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                    requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

                    var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);
                    response.EnsureSuccessStatusCode();

                    var responseData = JsonConvert.DeserializeObject<ApiResponse>(await response.Content.ReadAsStringAsync());
                    if (responseData.Status != "S")
                    {
                        _logger.LogError($"Error in response status: {responseData.Status}");
                        break;
                    }

                    if (responseData.Row != null)
                    {
                        foreach (var row in responseData.Row)
                        {
                            var item = new Payload1Response();
                            foreach (var field in row.Field)
                            {
                                if (field.Name == Name.PERNR)
                                    item.PERN = field.Value.String;
                                else if (field.Name == Name.FIELD1)
                                    item.Field1 = field.Value.String;
                                else if (field.Name == Name.LAST_UPDATE)
                                    item.LastUpdate = field.Value.String != null ? DateTime.Parse(field.Value.String) : (DateTime?)null;
                                // Add other fields as necessary
                            }
                            allData.Add(item);
                        }
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

        /// <summary>
        /// Fetches data for the second payload.
        /// </summary>
        /// <returns>A list of <see cref="Payload2Response"/> objects.</returns>
        private async Task<List<Payload2Response>> FetchData2Async()
        {
            var allData = new List<Payload2Response>();
            var page = 1;
            var pageSize = 5000;

            while (true)
            {
                try
                {
                    var data = CreatePayload(_payload2AppId, page, pageSize, "ESC", "=", "S");
                    var jsonPayload = JsonConvert.SerializeObject(data);
                    var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                    requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

                    var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);
                    response.EnsureSuccessStatusCode();

                    var responseData = JsonConvert.DeserializeObject<ApiResponse>(await response.Content.ReadAsStringAsync());
                    if (responseData.Status != "S")
                    {
                        _logger.LogError($"Error in response status: {responseData.Status}");
                        break;
                    }

                    if (responseData.Row != null)
                    {
                        foreach (var row in responseData.Row)
                        {
                            var item = new Payload2Response();
                            foreach (var field in row.Field)
                            {
                                if (field.Name == Name.PERNR)
                                    item.PERN = field.Value.String;
                                else if (field.Name == Name.FIELD2)
                                    item.Field2 = field.Value.String;
                                else if (field.Name == Name.LAST_UPDATE)
                                    item.LastUpdate = field.Value.String != null ? DateTime.Parse(field.Value.String) : (DateTime?)null;
                                // Add other fields as necessary
                            }
                            allData.Add(item);
                        }
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

        /// <summary>
        /// Runs the ETL process: fetches data from two APIs, joins the results, and upserts the joined data into the database.
        /// </summary>
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
                             select new ETLConsoleApp.Data.HCESIntegration
                             {
                                 PERN = d1.PERN,
                                 Field1 = d1.Field1,
                                 Field2 = d2.Field2,
                                 LastUpdate = d1.LastUpdate > d2.LastUpdate ? d1.LastUpdate : d2.LastUpdate
                                 // Add other fields as necessary
                             };

            foreach (var record in joinedData)
            {
                var existingRecord = await _dbContext.HCESIntegration.FindAsync(record.PERN);
                if (existingRecord == null)
                {
                    _dbContext.HCESIntegration.Add(record);
                }
                else
                {
                    existingRecord.Field1 = record.Field1;
                    existingRecord.Field2 = record.Field2;
                    existingRecord.LastUpdate = record.LastUpdate;
                    // Update other fields as necessary
                }
            }

            await _dbContext.SaveChangesAsync();
        }
    }
}