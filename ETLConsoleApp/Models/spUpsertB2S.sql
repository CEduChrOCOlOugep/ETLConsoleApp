-- Log table creation in STCS
CREATE TABLE STCS.UpsertLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    RequestID VARCHAR(MAX),
    RequestStatus VARCHAR(MAX),
    Operation VARCHAR(10),
    Timestamp DATETIME DEFAULT GETDATE()
);
GO

-- Create the stored procedure in STCS with logging
CREATE PROCEDURE STCS.MyUpsertProcedure
    @RequestID VARCHAR(MAX),
    @RequestStatus VARCHAR(MAX),
    @RowsAffected INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RequestStatusID INT

    -- Retrieve RequestStatusID from RequestStatusTable
    SELECT @RequestStatusID = RequestStatusID 
    FROM STCS.RequestStatusTable 
    WHERE RequestStatus = @RequestStatus;

    IF @RequestStatusID IS NULL
    BEGIN
        -- Handle the case where RequestStatusID is not found
        RAISERROR('RequestStatusID not found for RequestStatus: %s', 16, 1, @RequestStatus);
        RETURN;
    END

    -- Upsert operation
    MERGE INTO STCS.MyTable AS target
    USING (SELECT @RequestID AS RequestID, @RequestStatusID AS RequestStatusID) AS source
    ON (target.RequestID = source.RequestID)
    WHEN MATCHED THEN 
        UPDATE SET RequestStatusID = source.RequestStatusID
    WHEN NOT MATCHED THEN
        INSERT (RequestID, RequestStatusID)
        VALUES (source.RequestID, source.RequestStatusID);

    SET @RowsAffected = @@ROWCOUNT;

    -- Log the operation
    INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
    VALUES (@RequestID, @RequestStatus, 
            CASE WHEN EXISTS (SELECT 1 FROM STCS.MyTable WHERE RequestID = @RequestID) 
                 THEN 'UPDATE' ELSE 'INSERT' END);
END;
GO

-- If the temporary table exists, drop it
IF OBJECT_ID('tempdb..#BINS2Data') IS NOT NULL
    DROP TABLE #BINS2Data;
GO

-- Create a temporary table to hold the data from BINS2
SELECT RequestID, RequestStatus
INTO #BINS2Data
FROM BINS2.BINS2.MyView;
GO

-- Deduplicate the source data
SELECT RequestID, RequestStatus, COUNT(*) AS RowCount
INTO #DeduplicatedBINS2Data
FROM #BINS2Data
GROUP BY RequestID, RequestStatus;
GO

-- Perform the upsert operations
MERGE INTO STCS.MyTable AS target
USING (SELECT RequestID, RequestStatusID
       FROM #DeduplicatedBINS2Data src
       JOIN STCS.RequestStatusTable rst
       ON src.RequestStatus = rst.RequestStatus) AS source
ON (target.RequestID = source.RequestID)
WHEN MATCHED THEN 
    UPDATE SET RequestStatusID = source.RequestStatusID
WHEN NOT MATCHED THEN
    INSERT (RequestID, RequestStatusID)
    VALUES (source.RequestID, source.RequestStatusID);
GO

-- Log the operations
INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
SELECT RequestID, RequestStatus, 'UPSERT'
FROM #DeduplicatedBINS2Data;
GO

-- Cleanup temporary tables
IF OBJECT_ID('tempdb..#BINS2Data') IS NOT NULL
    DROP TABLE #BINS2Data;
IF OBJECT_ID('tempdb..#DeduplicatedBINS2Data') IS NOT NULL
    DROP TABLE #DeduplicatedBINS2Data;
GO

-- Validation Query: Verify the data in STCS
SELECT RequestID, RequestStatus
INTO #BINS2Data
FROM BINS2.BINS2.MyView;

SELECT mt.RequestID, rst.RequestStatus
INTO #STCSData
FROM STCS.STCS.MyTable mt
INNER JOIN STCS.STCS.RequestStatusTable rst ON mt.RequestStatusID = rst.RequestStatusID;

SELECT 
    a.RequestID AS BINS2_RequestID, 
    a.RequestStatus AS BINS2_RequestStatus, 
    b.RequestID AS STCS_RequestID, 
    b.RequestStatus AS STCS_RequestStatus
FROM #BINS2Data a
LEFT JOIN #STCSData b ON a.RequestID = b.RequestID
WHERE a.RequestStatus <> b.RequestStatus
   OR b.RequestID IS NULL;

DROP TABLE #BINS2Data;
DROP TABLE #STCSData;
GO

-- Row Count Validation
SELECT COUNT(*) AS BINS2RowCount
FROM BINS2.BINS2.MyView;

SELECT COUNT(*) AS STCSRowCount
FROM STCS.STCS.MyTable;
GO

-- Query to review logs
SELECT *
FROM STCS.UpsertLog
ORDER BY Timestamp DESC;
GO