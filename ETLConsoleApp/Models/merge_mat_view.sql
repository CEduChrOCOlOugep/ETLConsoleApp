-- CREATE INDEX idx_table1_id_ein ON table1(id, ein);
-- CREATE INDEX idx_table2_id_ein ON table2(id, ein);
CREATE PROCEDURE SyncTable2WithTable1
AS
BEGIN
    -- Ensure minimal logging for bulk operations
    SET NOCOUNT ON;

    -- Temporary table to hold data to be inserted
    CREATE TABLE #TempInsert (
        id INT,
        ein NVARCHAR(255),
        column1 NVARCHAR(255),
        column2 NVARCHAR(255)
        -- Add other columns as needed
    );

    -- Insert new records from table1 into #TempInsert
    INSERT INTO #TempInsert (id, ein, column1, column2)
    SELECT source.id, source.ein, source.column1, source.column2
    FROM table1 AS source
    LEFT JOIN table2 AS target
    ON source.ein = target.ein
    WHERE target.ein IS NULL;

    -- Perform the MERGE operation
    MERGE INTO table2 AS target
    USING table1 AS source
    ON target.ein = source.ein
    WHEN MATCHED AND 
         (target.id <> source.id OR 
          target.column1 <> source.column1 OR 
          target.column2 <> source.column2) THEN
        UPDATE SET
            target.id = source.id,
            target.column1 = source.column1,
            target.column2 = source.column2
            -- Add other columns as needed
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, ein, column1, column2)
        VALUES (source.id, source.ein, source.column1, source.column2);

    -- Insert new records into table2 from #TempInsert
    INSERT INTO table2 (id, ein, column1, column2)
    SELECT id, ein, column1, column2 FROM #TempInsert;

    -- Clean up
    DROP TABLE #TempInsert;

    SET NOCOUNT OFF;
END;
