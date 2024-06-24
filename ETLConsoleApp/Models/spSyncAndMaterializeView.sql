CREATE PROCEDURE SyncAndMaterializeView
AS
BEGIN
    -- Synchronize table1 with table2
    MERGE INTO table1 AS target
    USING table2 AS source
    ON target.id = source.id AND target.ein = source.ein
    WHEN MATCHED AND (source.col1 <> target.col1 OR source.col2 <> target.col2)
        THEN UPDATE SET 
            target.col1 = source.col1,
            target.col2 = source.col2
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (id, ein, col1, col2)
        VALUES (source.id, source.ein, source.col1, source.col2)
    WHEN NOT MATCHED BY SOURCE
        THEN DELETE;

    -- Truncate the materialized view table to remove old data
    TRUNCATE TABLE MaterializedView;

    -- Insert the latest data into the materialized view table
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
    INSERT INTO MaterializedView (id, ein, col1, col2, col3, col4)
    SELECT id, ein, col1, col2, col3, col4
    FROM RankedRecords
    WHERE rn = 1;
END
