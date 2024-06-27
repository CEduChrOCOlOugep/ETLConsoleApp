Certainly! Here is the updated script that includes an `IF EXISTS` check to drop the temporary table `#BINS2Data` if it exists:

### Consolidated Script with `IF EXISTS` Check

#### 1. Create the Log Table in STCS

```sql
-- Log table creation in STCS
CREATE TABLE STCS.UpsertLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    RequestID VARCHAR(MAX),
    RequestStatus VARCHAR(MAX),
    Operation VARCHAR(10),
    Timestamp DATETIME DEFAULT GETDATE()
);
GO
```

#### 2. Create the Stored Procedure in STCS

```sql
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
```

#### 3. Script to Execute the Stored Procedure for Each Row from BINS2

Here we avoid inserting into the identity column and add the `IF EXISTS` check for the temporary table:

```sql
-- If the temporary table exists, drop it
IF OBJECT_ID('tempdb..#BINS2Data') IS NOT NULL
    DROP TABLE #BINS2Data;
GO

-- Create a temporary table to hold the data from BINS2
SELECT RequestID, RequestStatus
INTO #BINS2Data
FROM BINS2.BINS2.MyView;
GO

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
    INSERT (RequestStatusID)  -- Only insert columns that are not identity
    VALUES (source.RequestStatusID);
GO

-- Log the operations
INSERT INTO STCS.UpsertLog (RequestID, RequestStatus, Operation)
SELECT RequestID, RequestStatus, 'UPSERT'
FROM #BINS2Data;
GO

-- Cleanup temporary table
IF OBJECT_ID('tempdb..#BINS2Data') IS NOT NULL
    DROP TABLE #BINS2Data;
GO
```

#### 4. Validation Queries

##### Validation Query: Verify the Data in STCS

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

##### Row Count Validation

```sql
-- Row Count Validation
SELECT COUNT(*) AS BINS2RowCount
FROM BINS2.BINS2.MyView;

SELECT COUNT(*) AS STCSRowCount
FROM STCS.STCS.MyTable;
GO
```

##### Query to Review Logs

```sql
-- Query to review logs
SELECT *
FROM STCS.UpsertLog
ORDER BY Timestamp DESC;
GO
```

### Summary

This updated script ensures:
1. **Log Table Creation**: The log table is available for the stored procedure.
2. **Stored Procedure Creation**: Sets up the procedure that will handle the upsert and logging operations.
3. **Set-Based Upsert Execution**: Avoids inserting into the identity column and includes checks for the existence of the temporary table before creation and cleanup.
4. **Validation Queries**: Checks the correctness of the upsert operations and reviews the logs.

Running these scripts in the specified order ensures that the necessary infrastructure is in place before executing the upsert operations and that validation steps are performed to verify the correctness of the process.