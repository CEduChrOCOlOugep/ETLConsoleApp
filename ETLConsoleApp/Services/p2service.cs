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
                            item.LastUpdate = ParseDateTime(field.Value.String);
                        else if (field.Name == Name.NODES)
                            item.Nodes = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.OCCCL)
                            item.Occcl = field.Value.String ?? string.Empty;
                        else if (field.Name == Name.SBDGT)
                            item.Sbdgt = ParseDateTime(field.Value.String);
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
/// Parses a date string to a nullable DateTime object.
/// </summary>
/// <param name="dateString">The date string to parse.</param>
/// <returns>A nullable DateTime object.</returns>
private DateTime? ParseDateTime(string dateString)
{
    if (string.IsNullOrWhiteSpace(dateString))
    {
        return null;
    }

    if (DateTime.TryParseExact(dateString, "yyyyMMddHHmmss", null, System.Globalization.DateTimeStyles.None, out var parsedDate))
    {
        return parsedDate;
    }

    _logger.LogError($"Invalid date format: {dateString}");
    return null;
}