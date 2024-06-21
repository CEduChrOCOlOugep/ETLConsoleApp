using RestSharp;
using System;
using System.Threading.Tasks;

public class ApiService
{
    private readonly RestClient _client;

    public ApiService()
    {
        _client = new RestClient();
    }

    public async Task<IRestResponse> SendPostRequestAsync(string url, object payload, string username, string password)
    {
        var request = new RestRequest(url, Method.Post);  // Use the full URL directly
        request.AddHeader("Content-Type", "application/json");
        
        var base64EncodedAuthenticationString = Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes($"{username}:{password}"));
        request.AddHeader("Authorization", $"Basic {base64EncodedAuthenticationString}");

        request.AddJsonBody(payload);

        return await _client.ExecuteAsync(request);
    }
}