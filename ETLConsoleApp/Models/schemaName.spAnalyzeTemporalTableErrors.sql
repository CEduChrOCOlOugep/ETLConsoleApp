CREATE PROCEDURE schemaName.AnalyzeTemporalTableErrors
AS
BEGIN
    WITH CurrentTable1 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table1
    ),
    CurrentTable2 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table2
    ),
    HistoricalTable1 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table1_history
    ),
    HistoricalTable2 AS (
        SELECT id, ein, col1, col2, col3, col4 FROM schemaName.table2_history
    )
    SELECT 
        'CurrentTable1 vs CurrentTable2' AS AnalysisType,
        c1.id, c1.ein, c1.col1 AS c1_col1, c2.col1 AS c2_col1, 
        c1.col2 AS c1_col2, c2.col2 AS c2_col2,
        c1.col3 AS c1_col3, c2.col3 AS c2_col3,
        c1.col4 AS c1_col4, c2.col4 AS c2_col4,
        CASE 
            WHEN c1.id IS NULL THEN 'Missing in CurrentTable1'
            WHEN c2.id IS NULL THEN 'Missing in CurrentTable2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM CurrentTable1 c1
    FULL OUTER JOIN CurrentTable2 c2 ON c1.id = c2.id AND c1.ein = c2.ein

    UNION ALL

    SELECT 
        'CurrentTable1 vs HistoricalTable1' AS AnalysisType,
        c1.id, c1.ein, c1.col1 AS c1_col1, h1.col1 AS h1_col1, 
        c1.col2 AS c1_col2, h1.col2 AS h1_col2,
        c1.col3 AS c1_col3, h1.col3 AS h1_col3,
        c1.col4 AS c1_col4, h1.col4 AS h1_col4,
        CASE 
            WHEN c1.id IS NULL THEN 'Missing in CurrentTable1'
            WHEN h1.id IS NULL THEN 'Missing in HistoricalTable1'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM CurrentTable1 c1
    FULL OUTER JOIN HistoricalTable1 h1 ON c1.id = h1.id AND c1.ein = h1.ein

    UNION ALL

    SELECT 
        'CurrentTable1 vs HistoricalTable2' AS AnalysisType,
        c1.id, c1.ein, c1.col1 AS c1_col1, h2.col1 AS h2_col1, 
        c1.col2 AS c1_col2, h2.col2 AS h2_col2,
        c1.col3 AS c1_col3, h2.col3 AS h2_col3,
        c1.col4 AS c1.col4, h2.col4 AS h2_col4,
        CASE 
            WHEN c1.id IS NULL THEN 'Missing in CurrentTable1'
            WHEN h2.id IS NULL THEN 'Missing in HistoricalTable2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM CurrentTable1 c1
    FULL OUTER JOIN HistoricalTable2 h2 ON c1.id = h2.id AND c1.ein = h2.ein

    UNION ALL

    SELECT 
        'CurrentTable2 vs HistoricalTable1' AS AnalysisType,
        c2.id, c2.ein, c2.col1 AS c2_col1, h1.col1 AS h1_col1, 
        c2.col2 AS c2_col2, h1.col2 AS h1_col2,
        c2.col3 AS c2_col3, h1.col3 AS h1.col3,
        c2.col4 AS c2.col4, h1.col4 AS h1.col4,
        CASE 
            WHEN c2.id IS NULL THEN 'Missing in CurrentTable2'
            WHEN h1.id IS NULL THEN 'Missing in HistoricalTable1'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM CurrentTable2 c2
    FULL OUTER JOIN HistoricalTable1 h1 ON c2.id = h1.id AND c2.ein = h1.ein

    UNION ALL

    SELECT 
        'CurrentTable2 vs HistoricalTable2' AS AnalysisType,
        c2.id, c2.ein, c2.col1 AS c2_col1, h2.col1 AS h2_col1, 
        c2.col2 AS c2_col2, h2.col2 AS h2_col2,
        c2.col3 AS c2_col3, h2.col3 AS h2.col3,
        c2.col4 AS c2.col4, h2.col4 AS h2.col4,
        CASE 
            WHEN c2.id IS NULL THEN 'Missing in CurrentTable2'
            WHEN h2.id IS NULL THEN 'Missing in HistoricalTable2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM CurrentTable2 c2
    FULL OUTER JOIN HistoricalTable2 h2 ON c2.id = h2.id AND c2.ein = h2.ein

    UNION ALL

    SELECT 
        'HistoricalTable1 vs HistoricalTable2' AS AnalysisType,
        h1.id, h1.ein, h1.col1 AS h1_col1, h2.col1 AS h2_col1, 
        h1.col2 AS h1_col2, h2.col2 AS h2_col2,
        h1.col3 AS h1.col3, h2.col3 AS h2.col3,
        h1.col4 AS h1.col4, h2.col4 AS h2.col4,
        CASE 
            WHEN h1.id IS NULL THEN 'Missing in HistoricalTable1'
            WHEN h2.id IS NULL THEN 'Missing in HistoricalTable2'
            ELSE 'Mismatch'
        END AS ErrorType
    FROM HistoricalTable1 h1
    FULL OUTER JOIN HistoricalTable2 h2 ON h1.id = h2.id AND h1.ein = h2.ein;
END
