

--Consolidated Script


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

    IF EXISTS (SELECT 1 FROM STCS.MyTable WHERE RequestID = @RequestID)
    BEGIN
        -- Update existing record
        UPDATE STCS.MyTable
        SET RequestStatusID = @RequestStatusID
        WHERE RequestID = @RequestID;

        SET @RowsAffected = @@ROWCOUNT;

        -- Log the update operation
        INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
        VALUES (@RequestID, @RequestStatus, 'UPDATE');
    END
    ELSE
    BEGIN
        -- Insert new record
        INSERT INTO STCS.MyTable (RequestID, RequestStatusID)
        VALUES (@RequestID, @RequestStatusID);

        SET @RowsAffected = @@ROWCOUNT;

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

### Optimization
###
The slow performance of the stored procedure execution before the validation queries could be due to several factors. Here are some common reasons and potential solutions:

### 1. **Cursor Usage**
Cursors can be slow, especially when processing a large number of rows. They perform row-by-row operations, which are inherently slower than set-based operations.

### 2. **Lack of Indexes**
If the tables involved in the stored procedure do not have proper indexing, the `SELECT`, `UPDATE`, and `INSERT` operations can be very slow.

### 3. **Complex Joins and Lookups**
The stored procedure performs lookups and joins, which can be slow if the underlying tables are large and not properly indexed.

### 4. **Logging Overhead**
Inserting into the log table for every operation can add overhead, especially if there are a lot of rows being processed.

### 5. **Network Latency**
If there are network issues or if the databases are on different servers (which you mentioned is not the case here), it can slow down the process.

### Optimizing the Stored Procedure Execution

Here are some steps to optimize the stored procedure execution:

#### 1. Use Set-Based Operations Instead of Cursors
Instead of using a cursor, you can perform the upsert operations using set-based operations. This is usually much faster.

#### 2. Ensure Proper Indexing
Make sure that `RequestID` and `RequestStatusID` columns are indexed in `STCS.MyTable` and `STCS.RequestStatusTable`.

#### 3. Batch Processing
If you must use row-by-row processing, consider processing the data in smaller batches.

### Example: Optimized Set-Based Operation

Here's an example of how you might rewrite the process to use set-based operations instead of a cursor:

#### Create a Temporary Table to Hold the Data from BINS2

```sql
-- Create a temporary table to hold the data from BINS2
SELECT RequestID, RequestStatus
INTO #BINS2Data
FROM BINS2.BINS2.MyView;
GO
```

#### Perform the Upsert Operations in a Set-Based Manner

```sql
-- Perform the upsert operations
MERGE INTO STCS.MyTable AS target
USING (SELECT RequestID, RequestStatusID
       FROM #BINS2Data src
       JOIN STCS.RequestStatusTable rst
       ON src.RequestStatus = rst.RequestStatus) AS source
ON (target.RequestID = source.RequestID)
WHEN MATCHED THEN 
    UPDATE SET RequestStatusID = source.RequestStatusID
WHEN NOT MATCHED THEN
    INSERT (RequestID, RequestStatusID)
    VALUES (source.RequestID, source.RequestStatusID);

-- Log the operations (if needed, can be optimized as well)
INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
SELECT RequestID, RequestStatus, 'UPSERT'
FROM #BINS2Data;
GO
```

### Validation Queries

#### Validation Query: Verify the Data in STCS

```sql
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
```

### Row Count Validation

```sql
-- Row Count Validation
SELECT COUNT(*) AS BINS2RowCount
FROM BINS2.BINS2.MyView;

SELECT COUNT(*) AS STCSRowCount
FROM STCS.STCS.MyTable;
GO
```

### Query to Review Logs

```sql
-- Query to review logs
SELECT *
FROM STCS.UpsertLog
ORDER BY Timestamp DESC;
GO
```

### Summary

This optimized approach avoids using a cursor and performs the upsert operations in a set-based manner, which should be much faster. Ensure that proper indexing is in place to further optimize the performance. By following these steps, you should see a significant improvement in the performance of the stored procedure execution.
