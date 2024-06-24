CREATE PROCEDURE schemaName.AnalyzeTemporalTableErrors
AS
BEGIN
    WITH hi1 AS (
        SELECT id, ein FROM schemaName.table1
    ),
    si2 AS (
        SELECT id, ein FROM schemaName.table2
    ),
    hih1 AS (
        SELECT id, ein FROM schemaName.table1_history
    ),
    sih2 AS (
        SELECT id, ein FROM schemaName.table2_history
    )
    SELECT DISTINCT
        'hi1 vs si2' AS AnalysisType,
        hi1.id, hi1.ein, si2.id AS si2_id, si2.ein AS si2_ein,
        CASE 
            WHEN hi1.id IS NULL THEN 'Missing in hi1'
            WHEN si2.id IS NULL THEN 'Missing in si2'
            WHEN hi1.ein <> si2.ein THEN 'EIN Mismatch'
            ELSE 'ID Mismatch'
        END AS ErrorType
    FROM hi1
    FULL OUTER JOIN si2 ON hi1.id = si2.id AND hi1.ein = si2.ein
    WHERE hi1.id IS NULL OR si2.id IS NULL OR hi1.ein <> si2.ein

    UNION ALL

    SELECT DISTINCT
        'hi1 vs hih1' AS AnalysisType,
        hi1.id, hi1.ein, hih1.id AS hih1_id, hih1.ein AS hih1_ein,
        CASE 
            WHEN hi1.id IS NULL THEN 'Missing in hi1'
            WHEN hih1.id IS NULL THEN 'Missing in hih1'
            WHEN hi1.ein <> hih1.ein THEN 'EIN Mismatch'
            ELSE 'ID Mismatch'
        END AS ErrorType
    FROM hi1
    FULL OUTER JOIN hih1 ON hi1.id = hih1.id AND hi1.ein = hih1.ein
    WHERE hi1.id IS NULL OR hih1.id IS NULL OR hi1.ein <> hih1.ein

    UNION ALL

    SELECT DISTINCT
        'hi1 vs sih2' AS AnalysisType,
        hi1.id, hi1.ein, sih2.id AS sih2_id, sih2.ein AS sih2_ein,
        CASE 
            WHEN hi1.id IS NULL THEN 'Missing in hi1'
            WHEN sih2.id IS NULL THEN 'Missing in sih2'
            WHEN hi1.ein <> sih2.ein THEN 'EIN Mismatch'
            ELSE 'ID Mismatch'
        END AS ErrorType
    FROM hi1
    FULL OUTER JOIN sih2 ON hi1.id = sih2.id AND hi1.ein = sih2.ein
    WHERE hi1.id IS NULL OR sih2.id IS NULL OR hi1.ein <> sih2.ein

    UNION ALL

    SELECT DISTINCT
        'si2 vs hih1' AS AnalysisType,
        si2.id, si2.ein, hih1.id AS hih1_id, hih1.ein AS hih1_ein,
        CASE 
            WHEN si2.id IS NULL THEN 'Missing in si2'
            WHEN hih1.id IS NULL THEN 'Missing in hih1'
            WHEN si2.ein <> hih1.ein THEN 'EIN Mismatch'
            ELSE 'ID Mismatch'
        END AS ErrorType
    FROM si2
    FULL OUTER JOIN hih1 ON si2.id = hih1.id AND si2.ein = hih1.ein
    WHERE si2.id IS NULL OR hih1.id IS NULL OR si2.ein <> hih1.ein

    UNION ALL

    SELECT DISTINCT
        'si2 vs sih2' AS AnalysisType,
        si2.id, si2.ein, sih2.id AS sih2_id, sih2.ein AS sih2_ein,
        CASE 
            WHEN si2.id IS NULL THEN 'Missing in si2'
            WHEN sih2.id IS NULL THEN 'Missing in sih2'
            WHEN si2.ein <> sih2.ein THEN 'EIN Mismatch'
            ELSE 'ID Mismatch'
        END AS ErrorType
    FROM si2
    FULL OUTER JOIN sih2 ON si2.id = sih2.id AND si2.ein = sih2.ein
    WHERE si2.id IS NULL OR sih2.id IS NULL OR si2.ein <> sih2.ein

    UNION ALL

    SELECT DISTINCT
        'hih1 vs sih2' AS AnalysisType,
        hih1.id, hih1.ein, sih2.id AS sih2_id, sih2.ein AS sih2_ein,
        CASE 
            WHEN hih1.id IS NULL THEN 'Missing in hih1'
            WHEN sih2.id IS NULL THEN 'Missing in sih2'
            WHEN hih1.ein <> sih2.ein THEN 'EIN Mismatch'
            ELSE 'ID Mismatch'
        END AS ErrorType
    FROM hih1
    FULL OUTER JOIN sih2 ON hih1.id = sih2.id AND hih1.ein = sih2.ein
    WHERE hih1.id IS NULL OR sih2.id IS NULL OR hih1.ein <> sih2.ein;
END
