public class Program
{
    public static async Task Main(string[] args)
    {
        var httpClient = new HttpClient();
        var apiService = new ApiService(httpClient);

        var url = "https://example.com/api/endpoint";
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

        var response = await apiService.SendPostRequestAsync(url, payload, username, password);

        Console.WriteLine(response);
    }
}