Certainly! Here is the consolidated script that includes the relevant steps and uses the correct database names and schemas:

```sql
-- Create the stored procedure in STCS
CREATE PROCEDURE STCS.MyUpsertProcedure
    @RequestID VARCHAR(MAX),
    @RequestStatus VARCHAR(MAX),
    @RowsAffected INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM STCS.MyTable WHERE RequestID = @RequestID)
    BEGIN
        -- Update existing record
        UPDATE STCS.MyTable
        SET RequestStatus = @RequestStatus
        WHERE RequestID = @RequestID;

        SET @RowsAffected = @@ROWCOUNT;
    END
    ELSE
    BEGIN
        -- Insert new record
        INSERT INTO STCS.MyTable (RequestID, RequestStatus)
        VALUES (@RequestID, @RequestStatus);

        SET @RowsAffected = @@ROWCOUNT;
    END
END;
GO

-- Log table creation in STCS
CREATE TABLE STCS.UpsertLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    RequestID VARCHAR(MAX),
    RequestStatus VARCHAR(MAX),
    Operation VARCHAR(10),
    Timestamp DATETIME DEFAULT GETDATE()
);
GO

-- Update the stored procedure to include logging
CREATE PROCEDURE STCS.MyUpsertProcedure
    @RequestID VARCHAR(MAX),
    @RequestStatus VARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM STCS.MyTable WHERE RequestID = @RequestID)
    BEGIN
        -- Update existing record
        UPDATE STCS.MyTable
        SET RequestStatus = @RequestStatus
        WHERE RequestID = @RequestID;

        -- Log the update operation
        INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
        VALUES (@RequestID, @RequestStatus, 'UPDATE');
    END
    ELSE
    BEGIN
        -- Insert new record
        INSERT INTO STCS.MyTable (RequestID, RequestStatus);

        -- Log the insert operation
        INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
        VALUES (@RequestID, @RequestStatus, 'INSERT');
    END
END;
GO

-- Script to execute the stored procedure for each row from BINS2
DECLARE @RequestID VARCHAR(MAX), @RequestStatus VARCHAR(MAX), @RowsAffected INT

DECLARE db_cursor CURSOR FOR
SELECT RequestID, RequestStatus
FROM BINS2.BINS2.MyView

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @RequestID, @RequestStatus

WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC STCS.MyUpsertProcedure @RequestID, @RequestStatus, @RowsAffected OUTPUT
    PRINT 'Rows affected: ' + CAST(@RowsAffected AS VARCHAR(MAX))
    FETCH NEXT FROM db_cursor INTO @RequestID, @RequestStatus
END

CLOSE db_cursor
DEALLOCATE db_cursor
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
```

This script covers the following:
1. **Creation of the stored procedure in STCS with logging.**
2. **Cursor to read data from BINS2 and call the stored procedure in STCS.**
3. **Validation query to compare data between BINS2 and STCS.**
4. **Row count validation.**
5. **Query to review the logs in the logging table.**

By running this script, you can ensure that the data is correctly upserted from BINS2 to STCS and validate the operation.