CREATE PROCEDURE SyncAndMaterializeView
AS
BEGIN
    -- Step 1: Synchronize table2 with table1
    MERGE INTO table2 AS target
    USING table1 AS source
    ON target.id = source.id AND target.ein = source.ein
    WHEN MATCHED AND (source.col1 <> target.col1 OR source.col2 <> target.col2)
        THEN UPDATE SET 
            target.col1 = source.col1,
            target.col2 = source.col2
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (id, ein, col1, col2)
        VALUES (source.id, source.ein, source.col1, source.col2);

    -- Step 2: Create a temporary table to hold the intermediate results
    CREATE TABLE #TempInsert (
        id INT,
        ein INT,
        col1 NVARCHAR(50),
        col2 NVARCHAR(50),
        col3 NVARCHAR(50),
        col4 NVARCHAR(50)
    );

    -- Step 3: Insert the ranked records into the temporary table
    WITH RankedRecords AS (
        SELECT
            t1.id,
            t1.ein,
            t1.col1,
            t1.col2,
            t2.col3,
            t2.col4,
            ROW_NUMBER() OVER (PARTITION BY t1.id ORDER BY t1.col1 DESC) AS rn
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.id AND t1.ein = t2.ein
    )
    INSERT INTO #TempInsert (id, ein, col1, col2, col3, col4)
    SELECT id, ein, col1, col2, col3, col4
    FROM RankedRecords
    WHERE rn = 1;

    -- Step 4: Merge the temporary table into the materialized view table
    MERGE INTO MaterializedView AS target
    USING #TempInsert AS source
    ON target.id = source.id AND target.ein = source.ein
    WHEN MATCHED AND (
            target.col1 <> source.col1 OR 
            target.col2 <> source.col2 OR
            target.col3 <> source.col3 OR
            target.col4 <> source.col4)
        THEN UPDATE SET
            target.col1 = source.col1,
            target.col2 = source.col2,
            target.col3 = source.col3,
            target.col4 = source.col4
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (id, ein, col1, col2, col3, col4)
        VALUES (source.id, source.ein, source.col1, source.col2, source.col3, source.col4);

    -- Step 5: Drop the temporary table
    DROP TABLE #TempInsert;
END
