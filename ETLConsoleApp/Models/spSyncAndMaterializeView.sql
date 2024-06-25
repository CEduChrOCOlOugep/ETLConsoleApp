CREATE PROCEDURE SyncAndMaterializeView
AS
BEGIN
    -- Ensure the temp table doesn't exist before creating it
    IF OBJECT_ID('tempdb..#TempInsert', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #TempInsert;
    END

    -- Step 1: Create a temporary table to hold the intermediate results
    CREATE TABLE #TempInsert (
        ID INT,
        NR NVARCHAR(50),
        D2 NVARCHAR(50),
        DT NVARCHAR(50),
        RS NVARCHAR(50)
    );

    -- Step 2: Insert the ranked records into the temporary table
    WITH RankedRecords AS (
        SELECT
            t1.ID,
            t1.NR,
            t1.D2,
            t1.DT,
            CASE WHEN t1.D2 = 'S' THEN 'HARQ' ELSE NULL END AS RS,
            ROW_NUMBER() OVER (PARTITION BY t1.ID ORDER BY t1.ID DESC) AS rn
        FROM table1 t1
        JOIN table2 t2 ON t1.ID = t2.SN
    )
    INSERT INTO #TempInsert (ID, NR, D2, DT, RS)
    SELECT ID, NR, D2, DT, RS
    FROM RankedRecords
    WHERE rn = 1;

    -- Step 3: Merge the temporary table into table2
    MERGE INTO table2 AS target
    USING #TempInsert AS source
    ON target.SN = source.ID
    WHEN MATCHED AND (
            (target.IN IS NULL AND source.NR IS NOT NULL) OR
            (target.PS IS NULL AND source.D2 IS NOT NULL) OR
            (target.AD IS NULL AND source.DT IS NOT NULL) OR
            (source.RS IS NOT NULL AND target.RS <> 'HARQ'))
        THEN UPDATE SET
            target.IN = CASE WHEN target.IN IS NULL THEN source.NR ELSE target.IN END,
            target.PS = CASE WHEN target.PS IS NULL THEN source.D2 ELSE target.PS END,
            target.AD = CASE WHEN target.AD IS NULL THEN source.DT ELSE target.AD END,
            target.RS = CASE WHEN source.RS IS NOT NULL THEN source.RS ELSE target.RS END
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (SN, IN, PS, AD, RS)
        VALUES (source.ID, source.NR, source.D2, source.DT, source.RS);

    -- Step 4: Drop the temporary table
    DROP TABLE #TempInsert;
END;