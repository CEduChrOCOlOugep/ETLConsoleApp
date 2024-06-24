CREATE PROCEDURE schemaName.AnalyzeTemporalTableErrors
AS
BEGIN
    WITH hi1 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table1
    ),
    si2 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table2
    ),
    hih1 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table1_history
    ),
    sih2 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table2_history
    )
    SELECT 
        'hi1 vs si2' AS AnalysisType,
        hi1.id, hi1.ein, hi1.col1 AS hi1_col1, si2.col1 AS si2_col1, 
        hi1.col2 AS hi1_col2, si2.col2 AS si2_col2,
        hi1.col3 AS hi1_col3, si2.col3 AS si2_col3,
        hi1.col4 AS hi1_col4, si2.col4 AS si2_col4,
        CASE 
            WHEN hi1.id IS NULL THEN 'Missing in hi1'
            WHEN si2.id IS NULL THEN 'Missing in si2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM hi1
    FULL OUTER JOIN si2 ON hi1.id = si2.id AND hi1.ein = si2.ein

    UNION ALL

    SELECT 
        'hi1 vs hih1' AS AnalysisType,
        hi1.id, hi1.ein, hi1.col1 AS hi1_col1, hih1.col1 AS hih1_col1, 
        hi1.col2 AS hi1_col2, hih1.col2 AS hih1_col2,
        hi1.col3 AS hi1_col3, hih1.col3 AS hih1_col3,
        hi1.col4 AS hi1.col4, hih1.col4 AS hih1.col4,
        CASE 
            WHEN hi1.id IS NULL THEN 'Missing in hi1'
            WHEN hih1.id IS NULL THEN 'Missing in hih1'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM hi1
    FULL OUTER JOIN hih1 ON hi1.id = hih1.id AND hi1.ein = hih1.ein

    UNION ALL

    SELECT 
        'hi1 vs sih2' AS AnalysisType,
        hi1.id, hi1.ein, hi1.col1 AS hi1_col1, sih2.col1 AS sih2_col1, 
        hi1.col2 AS hi1_col2, sih2.col2 AS sih2_col2,
        hi1.col3 AS hi1_col3, sih2.col3 AS sih2_col3,
        hi1.col4 AS hi1.col4, sih2.col4 AS sih2_col4,
        CASE 
            WHEN hi1.id IS NULL THEN 'Missing in hi1'
            WHEN sih2.id IS NULL THEN 'Missing in sih2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM hi1
    FULL OUTER JOIN sih2 ON hi1.id = sih2.id AND hi1.ein = sih2.ein

    UNION ALL

    SELECT 
        'si2 vs hih1' AS AnalysisType,
        si2.id, si2.ein, si2.col1 AS si2_col1, hih1.col1 AS hih1_col1, 
        si2.col2 AS si2_col2, hih1.col2 AS hih1_col2,
        si2.col3 AS si2_col3, hih1.col3 AS hih1_col3,
        si2.col4 AS si2.col4, hih1.col4 AS hih1.col4,
        CASE 
            WHEN si2.id IS NULL THEN 'Missing in si2'
            WHEN hih1.id IS NULL THEN 'Missing in hih1'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM si2
    FULL OUTER JOIN hih1 ON si2.id = hih1.id AND si2.ein = hih1.ein

    UNION ALL

    SELECT 
        'si2 vs sih2' AS AnalysisType,
        si2.id, si2.ein, si2.col1 AS si2_col1, sih2.col1 AS sih2_col1, 
        si2.col2 AS si2_col2, sih2.col2 AS sih2_col2,
        si2.col3 AS si2_col3, sih2.col3 AS sih2.col3,
        si2.col4 AS si2.col4, sih2.col4 AS sih2.col4,
        CASE 
            WHEN si2.id IS NULL THEN 'Missing in si2'
            WHEN sih2.id IS NULL THEN 'Missing in sih2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM si2
    FULL OUTER JOIN sih2 ON si2.id = sih2.id AND si2.ein = sih2.ein

    UNION ALL

    SELECT 
        'hih1 vs sih2' AS AnalysisType,
        hih1.id, hih1.ein, hih1.col1 AS hih1_col1, sih2.col1 AS sih2_col1, 
        hih1.col2 AS hih1_col2, sih2.col2 AS sih2_col2,
        hih1.col3 AS hih1.col3, sih2.col3 AS sih2.col3,
        hih1.col4 AS hih1.col4, sih2.col4 AS sih2.col4,
        CASE 
            WHEN hih1.id IS NULL THEN 'Missing in hih1'
            WHEN sih2.id IS NULL THEN 'Missing in sih2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM hih1
    FULL OUTER JOIN sih2 ON hih1.id = sih2.id AND hih1.ein = sih2.ein;
END
