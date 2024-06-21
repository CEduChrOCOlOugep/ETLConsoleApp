using RestSharp;
using System;
using System.Threading.Tasks;

public class Program
{
    public static async Task Main(string[] args)
    {
        var baseUrl = "https://";
        var apiService = new ApiService(baseUrl);

        var payload = new
        {
            appid = "4",
            pagenumber = "1",
            pagesize = "5000",
            filter = new
            {
                query = new[]
                {
                    new
                    {
                        fieldname = "L",
                        operand = ">=",
                        fieldvalue = "2"
                    }
                }
            }
        };

        var username = "yourUsername";
        var password = "yourPassword";

        RestResponse response = await apiService.SendPostRequestAsync(baseUrl, payload, username, password);

        Console.WriteLine(response.Content);
    }
}

public class ApiService
{
    private readonly RestClient _client;

    public ApiService(string baseUrl)
    {
        var options = new RestClientOptions(baseUrl)
        {
            MaxTimeout = -1,
        };
        _client = new RestClient(options);
    }

    public async Task<RestResponse> SendPostRequestAsync(string url, object payload, string username, string password)
    {
        var request = new RestRequest(url, Method.Post);
        var base64EncodedAuthenticationString = Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes($"{username}:{password}"));
        
        request.AddHeader("Authorization", $"Basic {base64EncodedAuthenticationString}");
        request.AddHeader("Content-Type", "application/json");

        var body = @$" {{
            ""appid"": ""{payload.appid}"",
            ""pagenumber"": ""{payload.pagenumber}"",
            ""pagesize"": ""{payload.pagesize}"",
            ""filter"": {{
                ""query"": [
                    {{
                        ""fieldname"": ""{payload.filter.query[0].fieldname}"",
                        ""operand"": ""{payload.filter.query[0].operand}"",
                        ""fieldvalue"": ""{payload.filter.query[0].fieldvalue}""
                    }}
                ]
            }}
        }}";

        request.AddStringBody(body, DataFormat.Json);

        return await _client.ExecuteAsync(request);
    }
}