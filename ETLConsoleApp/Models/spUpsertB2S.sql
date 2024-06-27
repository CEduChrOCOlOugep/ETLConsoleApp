-- Declare variables for the columns from the view
DECLARE @id INT, @name NVARCHAR(50), @value NVARCHAR(50), @date DATETIME, @description NVARCHAR(255), @status NVARCHAR(20)

-- Declare a cursor to iterate over the data from the view in DB1
DECLARE db_cursor CURSOR FOR
SELECT id, name, value, date, description, status
FROM DB1.dbo.MyView

OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @id, @name, @value, @date, @description, @status

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Call the stored procedure in DB2 for each row
    EXEC DB2.dbo.MyUpsertProcedure @id, @name, @value, @date, @description, @status
    
    FETCH NEXT FROM db_cursor INTO @id, @name, @value, @date, @description, @status
END

CLOSE db_cursor
DEALLOCATE db_cursor