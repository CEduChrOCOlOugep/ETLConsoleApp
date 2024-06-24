-- Create the indexed view
CREATE VIEW dbo.SynchronizedView
WITH SCHEMABINDING
AS
SELECT
    a.id,
    a.ein,
    a.column1,
    a.column2,
    a.latest_date
FROM (
    SELECT
        t1.id,
        t1.ein,
        t1.column1,
        t1.column2,
        MAX(t1.date_column) OVER (PARTITION BY t1.id) AS latest_date
    FROM
        dbo.table1 t1
    ) a
JOIN (
    SELECT
        t2.id,
        t2.ein,
        t2.column1,
        t2.column2,
        MAX(t2.date_column) OVER (PARTITION BY t2.id) AS latest_date
    FROM
        dbo.table2 t2
    ) b ON a.id = b.id AND a.ein = b.ein
WHERE a.latest_date = b.latest_date;

-- Create a unique clustered index on the view to materialize it
CREATE UNIQUE CLUSTERED INDEX IX_SynchronizedView ON dbo.SynchronizedView (id, ein);
