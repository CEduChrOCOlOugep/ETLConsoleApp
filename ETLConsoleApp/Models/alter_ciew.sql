ALTER VIEW dbo.MyView
AS
SELECT 
    dbo.MasterCleanString(column1, 'GENERAL') AS COLUMN1,
    dbo.MasterCleanString(column2, 'GENERAL') AS COLUMN2,
    LEFT(dbo.MasterCleanString(datetime_column, 'GENERAL'), 8) AS DATE_ONLY,
    dbo.MasterCleanString(column3, 'ADDRESS') AS COLUMN3,
    dbo.MasterCleanString(column4, 'CITY') AS COLUMN4,
    dbo.MasterCleanString(column5, 'STATE') AS COLUMN5,
    dbo.MasterCleanString(column6, 'ZIP5') AS ZIP5,
    dbo.MasterCleanString(column7, 'ZIP4') AS ZIP4
FROM 
    your_table;