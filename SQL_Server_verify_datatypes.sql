/* verify defined datatypes to current data */

SET NOCOUNT ON
SET ANSI_WARNINGS OFF

IF  EXISTS (SELECT * FROM tempdb.dbo.sysobjects 
WHERE id = OBJECT_ID(N'tempdb..#tmp_ValDT') 
AND type in (N'U'))
DROP TABLE #tmp_ValDT
GO

IF  EXISTS (SELECT * FROM tempdb.dbo.sysobjects 
WHERE id = OBJECT_ID(N'tempdb..#tmp_spTmp') 
AND type in (N'U'))
DROP TABLE #tmp_spTmp
GO

CREATE TABLE #tmp_ValDT
	(stmt nvarchar(max) NULL,
	min_val bigint NULL,
	max_val bigint NULL,
	min_date datetime NULL,
	max_date datetime NULL,
	max_len int NULL,
	min_mon money NULL,
	max_mon money NULL,
	rc bigint,
	table_name sysname,
	schema_name sysname,
	column_name sysname,
	TypeName sysname,
	recomm_DT sysname NULL,
	recomm_byte smallint NULL,
	max_length smallint,
	precision tinyint,
	scale tinyint)
GO

CREATE TABLE #tmp_spTmp
	(name nvarchar(128) NULL,
	rows char(11) NULL,
	reserved varchar(18) NULL,
	data varchar(18) NULL,
	index_size varchar(18) NULL,
	unused varchar(18))
GO

INSERT INTO #tmp_ValDT
SELECT '' AS stmt
, NULL -- min value not known yet
, NULL -- max value not known yet
, NULL -- min date not known yet
, NULL -- max date not known yet
, NULL -- max length not known yet
, NULL -- min money not known yet
, NULL -- max money not known yet
, NULL -- rowcount not known yet
, tbl.name AS table_name
, SCHEMA_NAME(tbl.schema_id) AS schema_name
, col.name AS column_name
, t.name AS TypeName
, NULL -- recommended DT not known yet
, NULL -- recommended DT bytes known yet
, col.max_length
, col.PRECISION
, col.scale
FROM sys.tables AS tbl
INNER JOIN sys.columns col ON tbl.OBJECT_ID = col.OBJECT_ID
INNER JOIN sys.types AS t ON col.user_type_id=t.user_type_id
--i am not bothering with user defined datatypes, or MAX types
WHERE t.is_user_defined = 0
AND t.is_assembly_type = 0
AND col.max_length <> -1
--will want to filter for specific types as well
--AND t.name IN ('tinyint', 'smallint', 'int', 'bigint')
--will want to make this dynamic by filtering for specfic table here
--AND tbl.name = 'Store'
ORDER BY schema_name, table_name;

DECLARE @SQL nvarchar(max)
DECLARE DT CURSOR
FOR 
SELECT table_name, schema_name, column_name, TypeName, max_length 
FROM #tmp_ValDT

DECLARE @table_name sysname, @schema_name sysname
DECLARE @column_name sysname, @TypeName sysname
DECLARE @min_val nvarchar(max), @max_val nvarchar(max)
DECLARE @min_date nvarchar(max), @max_date nvarchar(max)
DECLARE @min_mon nvarchar(max), @max_mon nvarchar(max)
DECLARE @max_length nvarchar(max), @rc nvarchar(max), @max_lenOUT nvarchar(max)
OPEN DT

FETCH NEXT FROM DT INTO @table_name, @schema_name, @column_name, @TypeName, @max_length
WHILE (@@fetch_status <> -1)
BEGIN
	IF (@@fetch_status <> -2)
	BEGIN

		IF @TypeName IN ('tinyint', 'smallint', 'int', 'bigint')
		BEGIN
		
			SET @SQL = 'SELECT @min_val = MIN(['+@column_name+']), @max_val = MAX(['+@column_name+']), @rc = COUNT(*) FROM ['+@schema_name+'].['+@table_name+']'		
			EXEC sp_executeSQL @SQL, N'@min_val bigint OUTPUT
			, @max_val bigint OUTPUT
			, @rc bigint OUTPUT', @min_val = @min_val OUTPUT, @max_val = @max_val OUTPUT, @rc = @rc OUTPUT

			IF CONVERT(BIGINT, @min_val) < -2147483648 OR CONVERT(BIGINT, @max_val) > 2147483648
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_val = @min_val, max_val = @max_val, rc = @rc,
						recomm_DT = 'bigint', recomm_byte = 8
					WHERE CURRENT OF DT
				END
			ELSE IF @min_val < -32768 OR @max_val > 32768
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_val = @min_val, max_val = @max_val, rc = @rc,
						recomm_DT = 'int', recomm_byte = 4
					WHERE CURRENT OF DT
				END
			ELSE IF @min_val < 0 OR @max_val > 255
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_val = @min_val, max_val = @max_val, rc = @rc,
						recomm_DT = 'smallint', recomm_byte = 2
					WHERE CURRENT OF DT
				END
			ELSE IF @min_val >= 0 AND @min_val <= 255 AND @max_val >= 0 AND @max_val <= 255
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_val = @min_val, max_val = @max_val, rc = @rc,
						recomm_DT = 'tinyint', recomm_byte = 1
					WHERE CURRENT OF DT
				END
			ELSE IF @min_val IS NULL OR @max_val IS NULL
				BEGIN
					PRINT 'unknown int'
				END
			ELSE
				BEGIN
					PRINT 'how did i get here int?'
				END
		END

		IF @TypeName IN ('smalldatetime', 'datetime')
		BEGIN
		
			SET @SQL = 'SELECT @min_date = MIN(['+@column_name+']), @max_date = MAX(['+@column_name+']), @rc = COUNT(*) FROM ['+@schema_name+'].['+@table_name+']'	
			EXEC sp_executeSQL @SQL, N'@min_date datetime OUTPUT
			, @max_date datetime OUTPUT
			, @rc bigint OUTPUT', @min_date = @min_date OUTPUT, @max_date = @max_date OUTPUT, @rc = @rc OUTPUT

			IF @min_date < CONVERT(DATETIME,'1900-01-01') OR @max_date > CONVERT(DATETIME,'2079-06-06')
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_date = @min_date, max_date = @max_date, rc = @rc,
						recomm_DT = 'datetime', recomm_byte = 8
					WHERE CURRENT OF DT
				END
			ELSE IF @min_date >= CONVERT(SMALLDATETIME,'1900-01-01') AND @min_date <= CONVERT(SMALLDATETIME,'2079-06-06') AND @max_date >= CONVERT(SMALLDATETIME,'1900-01-01') AND @max_date <= CONVERT(SMALLDATETIME,'2079-06-06')
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_date = @min_date, max_date = @max_date, rc = @rc,
						recomm_DT = 'smalldatetime', recomm_byte = 4
					WHERE CURRENT OF DT
				END
			ELSE IF @min_date IS NULL OR @max_date IS NULL
				BEGIN
					PRINT 'unknown dt'
				END
			ELSE
				BEGIN
					PRINT 'how did i get here dt?' 
				END
		END
		
		IF @TypeName IN ('smallmoney', 'money')
		BEGIN
		
			SET @SQL = 'SELECT @min_mon = MIN(['+@column_name+']), @max_mon = MAX(['+@column_name+']), @rc = COUNT(*) FROM ['+@schema_name+'].['+@table_name+']'		
			EXEC sp_executeSQL @SQL, N'@min_mon money OUTPUT
			, @max_mon money OUTPUT
			, @rc bigint OUTPUT', @min_mon = @min_mon OUTPUT, @max_mon = @max_mon OUTPUT, @rc = @rc OUTPUT

			IF @min_mon < CONVERT(MONEY,-214748.3648) OR @max_mon > CONVERT(MONEY,214748.3648)
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_mon = @min_mon, max_mon = @max_mon, rc = @rc,
						recomm_DT = 'money', recomm_byte = 8
					WHERE CURRENT OF DT
				END
			ELSE IF @min_mon >= CONVERT(MONEY,-214748.3648) AND @min_mon <= CONVERT(MONEY,214748.3648) AND @max_mon >= CONVERT(MONEY,-214748.3648) AND @max_mon <= CONVERT(MONEY,214748.3648)
				BEGIN
					UPDATE #tmp_ValDT
					SET stmt = @SQL, min_mon = @min_mon, max_mon = @max_mon, rc = @rc,
						recomm_DT = 'smallmoney', recomm_byte = 4
					WHERE CURRENT OF DT
				END
			ELSE IF @min_mon IS NULL OR @max_mon IS NULL
				BEGIN
					PRINT 'unknown money'
				END
			ELSE
				BEGIN
					PRINT 'how did i get here mn?' 
				END
		END		

		IF @TypeName IN ('nchar', 'nvarchar')
		BEGIN
		
		--for nchar and nvarchar we will verify if the data uses the extra bits
			SET @max_length = @max_length/2 --need this only for next statement D-SQL		
			SET @SQL = 'SELECT @rc = COUNT(*) FROM ['+@schema_name+'].['+@table_name+'] WHERE ['+@column_name+'] <> CAST(CAST(['+@column_name+'] AS varchar('+@max_length+')) AS nvarchar('+@max_length+'))'		
			EXEC sp_executeSQL @SQL, N'@rc bigint OUTPUT', @rc = @rc OUTPUT

			IF @rc = 0 --then we don't currently need the nvarchar overhead
				BEGIN
			
					SET @SQL = 'SELECT @max_length = MAX(LEN(['+@column_name+'])), @rc = COUNT(*) FROM ['+@schema_name+'].['+@table_name+'] '
					EXEC sp_executeSQL @SQL, N'@max_length int OUTPUT
					, @rc bigint OUTPUT', @max_length = @max_lenOUT OUTPUT, @rc = @rc OUTPUT
					
					UPDATE #tmp_ValDT
					SET stmt = @SQL, rc = @rc,
						recomm_DT = RIGHT(@Typename,LEN(@Typename)-1),
						recomm_byte = @max_lenOUT
					WHERE CURRENT OF DT
				END
				
			ELSE IF @rc > 0
				BEGIN
				
					SET @SQL = 'SELECT @max_length = MAX(LEN(['+@column_name+'])), @rc = COUNT(*) FROM ['+@schema_name+'].['+@table_name+'] '
					EXEC sp_executeSQL @SQL, N'@max_length int OUTPUT
					, @rc bigint OUTPUT', @max_length = @max_lenOUT OUTPUT, @rc = @rc OUTPUT
					
					UPDATE #tmp_ValDT
					SET stmt = @SQL, rc = @rc,
						recomm_DT = @Typename, recomm_byte = @max_lenOUT
					WHERE CURRENT OF DT
					
				END
			ELSE IF @rc IS NULL 
				BEGIN
					PRINT 'unknown string n'
				END
			ELSE
				BEGIN
					PRINT 'how did i get here n?' 
				END

		END		
		
	END
	FETCH NEXT FROM DT INTO @table_name, @schema_name, @column_name, @TypeName, @max_length
END

CLOSE DT
DEALLOCATE DT
GO

USE [GalacticWorks];
INSERT INTO #tmp_spTmp
EXEC sp_MSforeachtable 'EXECUTE sp_spaceused [?];'


/*
SELECT * FROM #tmp_ValDT

SELECT * FROM #tmp_spTmp
*/

SELECT schema_name + '.' + table_name AS [Tablename]
, column_name AS [ColumnName], TypeName AS [CurrentDT]
, max_length AS [Length], recomm_DT AS [RecommendedDT]
, recomm_byte AS [RecommendedLength]
, CASE WHEN recomm_DT NOT IN ('varchar', 'char')
	THEN ((max_length - recomm_byte) * rc)/(1024.0*1024.0) 
	ELSE (recomm_byte * 2)/(1024.0*1024.0) --because the 'n' would cost twice as much
	END AS [Space_Saved_MB]
FROM #tmp_ValDT
WHERE TypeName <> recomm_DT
AND recomm_byte <> 0
ORDER BY [Space_Saved_MB] DESC


SELECT vdt.table_name, SUM(vdt.max_length-vdt.recomm_byte) AS [row_savings_bytes] , SUM(max_length-recomm_byte) * spt.rows / 8192 AS [LIO_savings_pages]
FROM #tmp_ValDT vdt INNER JOIN #tmp_spTmp spt ON vdt.table_name = spt.name
GROUP BY vdt.table_name, spt.rows
ORDER BY 3 DESC
