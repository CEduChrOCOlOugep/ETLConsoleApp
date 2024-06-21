using System;
using System.Threading.Tasks;

public class Program
{
    public static async Task Main(string[] args)
    {
        var apiService = new ApiService();

        var url = "https://example.com/api/endpoint";  // Full URL
        var payload = new
        {
            appid = "id",
            pagenumber = "1",
            pagesize = "5000",
            filter = new
            {
                query = new[]
                {
                    new
                    {
                        fieldname = "LAST",
                        operand = ">=",
                        fieldvalue = "2"
                    }
                }
            }
        };

        var username = "yourUsername";
        var password = "yourPassword";

        IRestResponse response = await apiService.SendPostRequestAsync(url, payload, username, password);

        Console.WriteLine(response.Content);
    }
}