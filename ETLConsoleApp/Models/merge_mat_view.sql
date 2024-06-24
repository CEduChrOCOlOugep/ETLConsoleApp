CREATE PROCEDURE SyncTable2WithTable1
AS
BEGIN
    -- Merge statement to synchronize table2 with data from table1
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
        VALUES (source.id, source.ein, source.column1, source.column2)
        -- Add other columns as needed;

    -- Insert records from table1 into table2 where the ID does not match but EIN exists
    INSERT INTO table2 (id, ein, column1, column2)
    SELECT source.id, source.ein, source.column1, source.column2
    FROM table1 AS source
    LEFT JOIN table2 AS target
    ON source.ein = target.ein
    WHERE target.ein IS NULL;

END;
