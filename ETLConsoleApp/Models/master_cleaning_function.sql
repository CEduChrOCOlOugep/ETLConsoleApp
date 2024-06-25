CREATE FUNCTION dbo.MasterCleanString (@input NVARCHAR(MAX), @type NVARCHAR(50))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @cleaned NVARCHAR(MAX)
    
    -- Replace NULL values with empty strings
    SET @cleaned = ISNULL(@input, '')

    -- General cleaning: convert to lowercase, remove special characters, and trim whitespace
    SET @cleaned = LOWER(@cleaned)
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
    SET @cleaned = REPLACE(@cleaned, '\', '')
    SET @cleaned = REPLACE(@cleaned, '-', '')
    SET @cleaned = REPLACE(@cleaned, '=', '')
    SET @cleaned = REPLACE(@cleaned, ':', '')
    SET @cleaned = REPLACE(@cleaned, ';', '')
    SET @cleaned = REPLACE(@cleaned, '"', '')
    SET @cleaned = REPLACE(@cleaned, '''', '')
    SET @cleaned = LTRIM(RTRIM(@cleaned))

    -- Address specific cleaning
    IF @type = 'ADDRESS'
    BEGIN
        SET @cleaned = REPLACE(@cleaned, 'st', 'street')
        SET @cleaned = REPLACE(@cleaned, 'rd', 'road')
        SET @cleaned = REPLACE(@cleaned, 'ave', 'avenue')
        SET @cleaned = REPLACE(@cleaned, 'blvd', 'boulevard')
        SET @cleaned = REPLACE(@cleaned, 'dr', 'drive')
        SET @cleaned = REPLACE(@cleaned, 'ln', 'lane')
    END

    -- City specific cleaning
    IF @type = 'CITY'
    BEGIN
        -- Additional rules for cities if needed
    END

    -- State specific cleaning
    IF @type = 'STATE'
    BEGIN
        -- Ensure state codes are in the correct format
        IF LEN(@cleaned) = 2 SET @cleaned = UPPER(@cleaned)
    END

    -- ZIP code specific cleaning
    IF @type = 'ZIP5'
    BEGIN
        -- Remove non-numeric characters and pad with leading zeros
        SET @cleaned = REPLACE(@cleaned, ' ', '')
        SET @cleaned = REPLACE(@cleaned, '-', '')
        WHILE PATINDEX('%[^0-9]%', @cleaned) > 0
            SET @cleaned = STUFF(@cleaned, PATINDEX('%[^0-9]%', @cleaned), 1, '')
        SET @cleaned = RIGHT(REPLICATE('0', 5) + @cleaned, 5)
    END

    IF @type = 'ZIP4'
    BEGIN
        -- Ensure ZIP+4 format
        SET @cleaned = REPLACE(@cleaned, ' ', '')
        SET @cleaned = REPLACE(@cleaned, '-', '')
        WHILE PATINDEX('%[^0-9]%', @cleaned) > 0
            SET @cleaned = STUFF(@cleaned, PATINDEX('%[^0-9]%', @cleaned), 1, '')
        IF LEN(@cleaned) = 9 
            SET @cleaned = LEFT(@cleaned, 5) + '-' + RIGHT(@cleaned, 4)
        ELSE 
            SET @cleaned = NULL -- or handle invalid ZIP+4s accordingly
    END

    RETURN @cleaned
END
GO