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
SELECT RequestID, RequestStatus
INTO #DeduplicatedBINS2Data
FROM #BINS2Data
GROUP BY RequestID, RequestStatus;
GO

-- Enable IDENTITY_INSERT
SET IDENTITY_INSERT STCS.MyTable ON;
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
    INSERT (RequestID, RequestStatusID)  -- Include the identity column
    VALUES (source.RequestID, source.RequestStatusID);
GO

-- Disable IDENTITY_INSERT
SET IDENTITY_INSERT STCS.MyTable OFF;
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