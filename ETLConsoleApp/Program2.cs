using System;
using System.Threading.Tasks;

public class Program
{
    public static async Task Main(string[] args)
    {
        var baseUrl = "https://example.com/api/";
        var endpoint = "endpoint";
        var apiService = new ApiService(baseUrl);

        var payload = new
        {
            appId = "yourAppId",
            pageNumber = 1,
            pageSize = 10,
            filter = "yourFilter",
            query = new
            {
                filename = "yourFilename",
                operand = "yourOperand",
                fieldValue = "yourFieldValue"
            }
        };

        var username = "yourUsername";
        var password = "yourPassword";

        IRestResponse response = await apiService.SendPostRequestAsync(endpoint, payload, username, password);

        Console.WriteLine(response.Content);
    }
}