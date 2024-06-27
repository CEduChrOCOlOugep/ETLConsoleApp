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

    -- Update operation
    UPDATE STCS.Requests
    SET RequestStatusID = @RequestStatusID
    WHERE RequestID = @RequestID;

    SET @RowsAffected = @@ROWCOUNT;

    -- Log the operation
    INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
    VALUES (@RequestID, @RequestStatus, 
            CASE WHEN @RowsAffected > 0 THEN 'UPDATE' ELSE 'NO CHANGE' END);
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
SELECT DISTINCT RequestID, RequestStatus
INTO #DeduplicatedBINS2Data
FROM #BINS2Data;
GO

-- Perform the update operations
DECLARE @RequestID VARCHAR(MAX), @RequestStatus VARCHAR(MAX), @RowsAffected INT;

DECLARE db_cursor CURSOR FOR
SELECT RequestID, RequestStatus
FROM #DeduplicatedBINS2Data;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @RequestID, @RequestStatus;

WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC STCS.MyUpsertProcedure @RequestID, @RequestStatus, @RowsAffected OUTPUT;
    FETCH NEXT FROM db_cursor INTO @RequestID, @RequestStatus;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;
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
FROM STCS.Requests mt
INNER JOIN STCS.ListRequestsStatus rst ON mt.RequestStatusID = rst.RequestStatusID;

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
FROM STCS.Requests;
GO

-- Query to review logs
SELECT *
FROM STCS.UpsertLog
ORDER BY Timestamp DESC;
GO