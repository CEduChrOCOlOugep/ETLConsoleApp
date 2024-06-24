CREATE VIEW SynchronizedView AS
WITH SyncTable2 AS (
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
        -- Add other columns as needed
    OUTPUT $action AS Action, inserted.*, deleted.*;
)

-- Now define the main query for the view, joining the synchronized tables
SELECT 
    t1.id AS table1_id,
    t1.ein AS table1_ein,
    t1.column1 AS table1_column1,
    t1.column2 AS table1_column2,
    t2.id AS table2_id,
    t2.ein AS table2_ein,
    t2.column1 AS table2_column1,
    t2.column2 AS table2_column2
FROM 
    table1 t1
LEFT JOIN 
    table2 t2 ON t1.id = t2.id AND t1.ein = t2.ein;

-- EXEC SyncTable2WithTable1;
-- SELECT * FROM SynchronizedView;

