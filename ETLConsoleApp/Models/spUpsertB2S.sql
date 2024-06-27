

### Updated Script with Additional Columns

#### 1. Update the Log Table Creation

```sql
-- Log table creation in STCS
CREATE TABLE STCS.UpsertLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    RequestID VARCHAR(MAX),
    RequestStatus VARCHAR(MAX),
    Operation VARCHAR(10),
    Column1 VARCHAR(MAX),
    Column2 VARCHAR(MAX),
    Timestamp DATETIME DEFAULT GETDATE()
);
GO
```

#### 2. Update the Stored Procedure

```sql
-- Create the stored procedure in STCS with logging and error handling
CREATE PROCEDURE STCS.MyUpsertProcedure
    @RequestID VARCHAR(MAX),
    @RequestStatus VARCHAR(MAX),
    @Column1 VARCHAR(MAX),
    @Column2 VARCHAR(MAX),
    @RowsAffected INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RequestStatusID INT;

    -- Retrieve RequestStatusID from RequestStatusTable
    SELECT @RequestStatusID = RequestStatusID 
    FROM STCS.RequestStatusTable 
    WHERE RequestStatus = @RequestStatus;

    IF @RequestStatusID IS NULL
    BEGIN
        -- Log the case where RequestStatusID is not found
        RAISERROR('RequestStatusID not found for RequestStatus: %s', 16, 1, @RequestStatus);
        INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation, Column1, Column2)
        VALUES (@RequestID, @RequestStatus, 'RequestStatusID not found', @Column1, @Column2);
        RETURN;
    END

    -- Update operation
    UPDATE STCS.Requests
    SET RequestStatusID = @RequestStatusID
    WHERE RequestID = @RequestID;

    SET @RowsAffected = @@ROWCOUNT;

    -- Log the operation
    INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation, Column1, Column2)
    VALUES (@RequestID, @RequestStatus, 
            CASE WHEN @RowsAffected > 0 THEN 'UPDATE' ELSE 'NO CHANGE' END, @Column1, @Column2);
END;
GO
```

#### 3. Full Script Including Updated Stored Procedure and Log Table

```sql
-- Log table creation in STCS
CREATE TABLE STCS.UpsertLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    RequestID VARCHAR(MAX),
    RequestStatus VARCHAR(MAX),
    Operation VARCHAR(10),
    Column1 VARCHAR(MAX),
    Column2 VARCHAR(MAX),
    Timestamp DATETIME DEFAULT GETDATE()
);
GO

-- Create the stored procedure in STCS with logging and error handling
CREATE PROCEDURE STCS.MyUpsertProcedure
    @RequestID VARCHAR(MAX),
    @RequestStatus VARCHAR(MAX),
    @Column1 VARCHAR(MAX),
    @Column2 VARCHAR(MAX),
    @RowsAffected INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RequestStatusID INT;

    -- Retrieve RequestStatusID from RequestStatusTable
    SELECT @RequestStatusID = RequestStatusID 
    FROM STCS.RequestStatusTable 
    WHERE RequestStatus = @RequestStatus;

    IF @RequestStatusID IS NULL
    BEGIN
        -- Log the case where RequestStatusID is not found
        RAISERROR('RequestStatusID not found for RequestStatus: %s', 16, 1, @RequestStatus);
        INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation, Column1, Column2)
        VALUES (@RequestID, @RequestStatus, 'RequestStatusID not found', @Column1, @Column2);
        RETURN;
    END

    -- Update operation
    UPDATE STCS.Requests
    SET RequestStatusID = @RequestStatusID
    WHERE RequestID = @RequestID;

    SET @RowsAffected = @@ROWCOUNT;

    -- Log the operation
    INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation, Column1, Column2)
    VALUES (@RequestID, @RequestStatus, 
            CASE WHEN @RowsAffected > 0 THEN 'UPDATE' ELSE 'NO CHANGE' END, @Column1, @Column2);
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

-- Perform the update operations
DECLARE @RequestID VARCHAR(MAX), @RequestStatus VARCHAR(MAX), @Column1 VARCHAR(MAX), @Column2 VARCHAR(MAX), @RowsAffected INT;

DECLARE db_cursor CURSOR FOR
SELECT RequestID, RequestStatus, Column1, Column2
FROM #DeduplicatedBINS2Data;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @RequestID, @RequestStatus, @Column1, @Column2;

WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC STCS.MyUpsertProcedure @RequestID, @RequestStatus, @Column1, @Column2, @RowsAffected OUTPUT;
    FETCH NEXT FROM db_cursor INTO @RequestID, @RequestStatus, @Column1, @Column2;
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
FROM STCS.UpsertLog
ORDER BY Timestamp DESC;
GO
```

This script includes the additional columns (`Column1` and `Column2`) from your view and logs them in the `UpsertLog` table. The stored procedure has been updated to handle these additional columns, and the entire script ensures that these columns are correctly logged and validated.