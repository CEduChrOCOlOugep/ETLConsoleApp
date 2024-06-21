private async Task<List<Payload1Response>> FetchData1Async()
{
    var allData = new List<Payload1Response>();
    var page = 1;
    var pageSize = 5000;

    while (true)
    {
        try
        {
            var data = CreatePayload(_payload1AppId, page, pageSize, "LAST_UPDATE", ">=", "20240601000000");
            var jsonPayload = JsonConvert.SerializeObject(data);
            var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

            _logger.LogInformation($"Sending request: {jsonPayload}");

            var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);

            if (!response.IsSuccessStatusCode)
            {
                var responseContent = await response.Content.ReadAsStringAsync();
                _logger.LogError($"Error response: {responseContent}");
                response.EnsureSuccessStatusCode();
            }

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
                        else if (field.Name == Name.PERID)
                            item.PerId = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.LAST_UPDATE)
                            item.LastUpdate = field.Value.String ?? string.Empty;
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
            var data = CreatePayload(_payload2AppId, page, pageSize, "LAST_UPDATE", ">=", "20240601000000");
            var jsonPayload = JsonConvert.SerializeObject(data);
            var requestContent = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            requestContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", _encodedCredentials);

            _logger.LogInformation($"Sending request: {jsonPayload}");

            var response = await _httpClient.PostAsync(_apiBaseUrl, requestContent);

            if (!response.IsSuccessStatusCode)
            {
                var responseContent = await response.Content.ReadAsStringAsync();
                _logger.LogError($"Error response: {responseContent}");
                response.EnsureSuccessStatusCode();
            }

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
                        else if (field.Name == Name.ACTON)
                            item.Acton = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.DYFIN)
                            item.Dyfin = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.ESC02)
                            item.Esc02 = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.LAST_UPDATE)
                            item.LastUpdate = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.NODES)
                            item.Nodes = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.OCCCL)
                            item.Occcl = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.SBDGT)
                            item.Sbdgt = field.Value.String ?? string.Empty;
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