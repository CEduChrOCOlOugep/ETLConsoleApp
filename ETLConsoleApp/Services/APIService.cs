using RestSharp;
using System;
using System.Threading.Tasks;

public class ApiService
{
    private readonly RestClient _client;

    public ApiService(string baseUrl)
    {
        _client = new RestClient(baseUrl);
    }

    public async Task<IRestResponse> SendPostRequestAsync(string endpoint, object payload, string username, string password)
    {
        var request = new RestRequest(endpoint, Method.POST);
        request.AddHeader("Content-Type", "application/json");
        
        var base64EncodedAuthenticationString = Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes($"{username}:{password}"));
        request.AddHeader("Authorization", $"Basic {base64EncodedAuthenticationString}");

        request.AddJsonBody(payload);

        return await _client.ExecuteAsync(request);
    }
}
