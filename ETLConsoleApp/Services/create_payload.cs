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