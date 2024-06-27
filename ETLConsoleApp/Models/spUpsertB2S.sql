-- Use the BINS2 database
USE BINS2;
GO

-- Log table creation in BINS2
CREATE TABLE BINS2.UpsertLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    RequestID VARCHAR(MAX),
    RequestStatus VARCHAR(MAX),
    Operation VARCHAR(10),
    Column1 VARCHAR(MAX),
    Column2 VARCHAR(MAX),
    Timestamp DATETIME DEFAULT GETDATE()
);
GO

-- Create the stored procedure in BINS2 with logging and error handling
CREATE PROCEDURE BINS2.MyUpsertProcedure
AS
BEGIN
    SET NOCOUNT ON;

    -- Temporary table to store results of the join
    IF OBJECT_ID('tempdb..#TempResult') IS NOT NULL
        DROP TABLE #TempResult;
    
    CREATE TABLE #TempResult (
        RequestID VARCHAR(MAX),
        RequestStatusID INT,
        Column1 VARCHAR(MAX),
        Column2 VARCHAR(MAX)
    );

    -- Join the source data with the RequestStatusTable to get the RequestStatusID
    INSERT INTO #TempResult (RequestID, RequestStatusID, Column1, Column2)
    SELECT src.RequestID, rst.RequestStatusID, src.Column1, src.Column2
    FROM #DeduplicatedBINS2Data src
    LEFT JOIN STCS.RequestStatusTable rst
    ON src.RequestStatus = rst.RequestStatus
    WHERE rst.RequestStatusID IS NOT NULL;

    -- Update operation in STCS.Requests using the temporary result
    UPDATE target
    SET target.RequestStatusID = source.RequestStatusID
    FROM STCS.Requests AS target
    INNER JOIN #TempResult AS source
    ON target.RequestID = source.RequestID;

    -- Log the operation in BINS2.UpsertLog
    INSERT INTO BINS2.UpsertLog (RequestID, RequestStatus, Operation, Column1, Column2)
    SELECT src.RequestID, src.RequestStatus, 
           CASE WHEN target.RequestID IS NOT NULL THEN 'UPDATE' ELSE 'NO CHANGE' END, 
           src.Column1, src.Column2
    FROM #DeduplicatedBINS2Data src
    LEFT JOIN #TempResult trg
    ON src.RequestID = trg.RequestID
    LEFT JOIN STCS.Requests target
    ON src.RequestID = target.RequestID;

    -- Log the cases where RequestStatusID is not found
    INSERT INTO BINS2.UpsertLog (RequestID, RequestStatus, Operation, Column1, Column2)
    SELECT src.RequestID, src.RequestStatus, 'RequestStatusID not found', src.Column1, src.Column2
    FROM #DeduplicatedBINS2Data src
    LEFT JOIN STCS.RequestStatusTable rst
    ON src.RequestStatus = rst.RequestStatus
    WHERE rst.RequestStatusID IS NULL;

    -- Cleanup temporary table
    IF OBJECT_ID('tempdb..#TempResult') IS NOT NULL
        DROP TABLE #TempResult;
END;
GO

-- If the temporary table exists, drop it
IF OBJECT_ID('tempdb..#BINS2Data') IS NOT NULL
    DROP TABLE #BINS2Data;
GO

-- Create a temporary table to hold the data from BINS2
SELECT RequestID, RequestStatus, Column1, Column2
INTO #BINS2Data
FROM BINS2.BINS2.MyView;
GO

-- Deduplicate the source data
SELECT DISTINCT RequestID, RequestStatus, Column1, Column2
INTO #DeduplicatedBINS2Data
FROM #BINS2Data;
GO

-- Perform the update operations using the optimized stored procedure
EXEC BINS2.MyUpsertProcedure;
GO

-- Cleanup temporary tables
IF OBJECT_ID('tempdb..#BINS2Data') IS NOT NULL
    DROP TABLE #BINS2Data;
IF OBJECT_ID('tempdb..#DeduplicatedBINS2Data') IS NOT NULL
    DROP TABLE #DeduplicatedBINS2Data;
GO

-- Validation Query: Verify the data in STCS
SELECT RequestID, RequestStatus, Column1, Column2
INTO #BINS2Data
FROM BINS2.BINS2.MyView;

SELECT mt.RequestID, rst.RequestStatus, mt.Column1, mt.Column2
INTO #STCSData
FROM STCS.Requests mt
INNER JOIN STCS.ListRequestsStatus rst ON mt.RequestStatusID = rst.RequestStatusID;

SELECT 
    a.RequestID AS BINS2_RequestID, 
    a.RequestStatus AS BINS2_RequestStatus, 
    b.RequestID AS STCS_RequestID, 
    b.RequestStatus AS STCS_RequestStatus,
    a.Column1 AS BINS2_Column1,
    a.Column2 AS BINS2_Column2,
    b.Column1 AS STCS_Column1,
    b.Column2 AS STCS_Column2
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
FROM BINS2.UpsertLog
ORDER BY Timestamp DESC;
GO