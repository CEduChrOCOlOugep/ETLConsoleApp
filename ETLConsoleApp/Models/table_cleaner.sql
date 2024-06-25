ALTER VIEW dbo.MyView
AS
SELECT 
    LOWER(TRANSLATE(column1, '!@#$%^&*()_+[]{}|;:,.<>?/`~', '')) AS COLUMN1,
    LOWER(TRANSLATE(column2, '!@#$%^&*()_+[]{}|;:,.<>?/`~', '')) AS COLUMN2,
    LEFT(LOWER(TRANSLATE(datetime_column, '!@#$%^&*()_+[]{}|;:,.<>?/`~', '')), 8) AS DATE_ONLY,
    LOWER(TRANSLATE(column3, '!@#$%^&*()_+[]{}|;:,.<>?/`~', '')) AS COLUMN3,
    LOWER(TRANSLATE(column4, '!@#$%^&*()_+[]{}|;:,.<>?/`~', '')) AS COLUMN4
FROM 
    your_table;