CREATE FUNCTION dbo.CleanString (@input NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @cleaned NVARCHAR(MAX)
    SET @cleaned = LOWER(@input)
    
    -- Remove special characters
    SET @cleaned = REPLACE(@cleaned, '!', '')
    SET @cleaned = REPLACE(@cleaned, '@', '')
    SET @cleaned = REPLACE(@cleaned, '#', '')
    SET @cleaned = REPLACE(@cleaned, '$', '')
    SET @cleaned = REPLACE(@cleaned, '%', '')
    SET @cleaned = REPLACE(@cleaned, '^', '')
    SET @cleaned = REPLACE(@cleaned, '&', '')
    SET @cleaned = REPLACE(@cleaned, '*', '')
    SET @cleaned = REPLACE(@cleaned, '(', '')
    SET @cleaned = REPLACE(@cleaned, ')', '')
    SET @cleaned = REPLACE(@cleaned, '_', '')
    SET @cleaned = REPLACE(@cleaned, '+', '')
    SET @cleaned = REPLACE(@cleaned, '[', '')
    SET @cleaned = REPLACE(@cleaned, ']', '')
    SET @cleaned = REPLACE(@cleaned, '{', '')
    SET @cleaned = REPLACE(@cleaned, '}', '')
    SET @cleaned = REPLACE(@cleaned, '|', '')
    SET @cleaned = REPLACE(@cleaned, ';', '')
    SET @cleaned = REPLACE(@cleaned, ':', '')
    SET @cleaned = REPLACE(@cleaned, ',', '')
    SET @cleaned = REPLACE(@cleaned, '.', '')
    SET @cleaned = REPLACE(@cleaned, '<', '')
    SET @cleaned = REPLACE(@cleaned, '>', '')
    SET @cleaned = REPLACE(@cleaned, '?', '')
    SET @cleaned = REPLACE(@cleaned, '/', '')
    SET @cleaned = REPLACE(@cleaned, '`', '')
    SET @cleaned = REPLACE(@cleaned, '~', '')

    RETURN @cleaned
END
GO