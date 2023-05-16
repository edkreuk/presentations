CREATE OR ALTER PROCEDURE [execution].[CreateLakehouseTables] (
	@SourceFilePath				nvarchar(4000)   --full path and filename (possible with wildcards to the source files)
	,@SourceLayer				nvarchar(255)	-- (raw|deltalake)
	,@TargetSchema				nvarchar(255) 
	,@TargetTable				nvarchar(255)
	,@DataStoreExcludeColumns	nvarchar(max)	--; seperated list with exclude columns
	,@DataStoreIncludeColumns	nvarchar(max) = NULL	--; seperated list with include columns
	,@CustomSchema				nvarchar(max)	--list with unions: 'UNION SELECT ''CountryID'', ''nvarchar(max)'' '
)
AS

/**********************************************************************************************************
* Purpose:		Create Lake house views
* 
* Revision Date/Time:
*  Erwin de Kreuk (InSpark) - Initial creation of SP

**********************************************************************************************************/
BEGIN

	/*
	DECLARE @SourceFilePath				nvarchar(4000)		= 'https://labseuwdvlmdloxgn01.blob.core.windows.net/intermediate/DeltaLake/AFAS/AFAS/Projecten/'
	DECLARE @SourceLayer				nvarchar(50)		= 'deltalake'
	DECLARE @TargetSchema				nvarchar(255)		= 'AFAS'
	DECLARE @TargetTable				nvarchar(255)		= 'projecten'
	DECLARE @CustomSchema				nvarchar(max)		= '' --'UNION SELECT ''CountryID'', ''nvarchar(max)'' ' --'@{activity(''Get Custom Schema'').output.firstRow?.DataTypes}'
	DECLARE @DataStoreExcludeColumns	nvarchar(max)		= NULL --'' --'CountryID;CountryName'; --'@{if(not(empty(pipeline().parameters.DataStoreExcludeColumns)), replace(pipeline().parameters.DataStoreExcludeColumns, ';', ''','''), '')}'
	DECLARE @DataStoreIncludeColumns	nvarchar(max)		= NULL --'ProjectId;Description'
	-- */

	DECLARE @Exec						AS nvarchar(max)		= NULL
		,	@SelectSchema				AS nvarchar(max)		= NULL
		,	@WithSchema					AS nvarchar(max)		= NULL
		,	@Debug						AS bit					= 1 -- Show debug output
		,	@SourceType					AS nvarchar(50)			= 'DELTA'

	IF @SourceLayer = 'raw'
	BEGIN
		SET @SourceType = 'PARQUET'
		SET @SourceFilePath = '''' + @SourceFilePath + ''', ''' + @SourceFilePath + '/**''';
	END
	ELSE
	BEGIN
		SET @SourceFilePath = '''' + @SourceFilePath + '''';
	END

	-- create view so we can get the metadata
	SET @Exec = 'CREATE OR ALTER VIEW [DeltaLake].' + QUOTENAME(@TargetSchema + '_' + @TargetTable) + ' AS
				-- View created: ' + CONVERT(nvarchar(23), GETDATE(), 25) + ' 
				SELECT TOP 1 *
				FROM
					OPENROWSET(
						BULK (' + @SourceFilePath + '),
						FORMAT=''' + @SourceType + '''
					) AS [result]'

	IF @Debug = 1 PRINT '@Exec: ' + @Exec
    EXEC sys.sp_executesql @Exec;

	-- create temp table for custom schema
	IF OBJECT_ID('tempdb..#CustomSchemaTable') IS NOT NULL 
		DROP TABLE #CustomSchemaTable

	CREATE TABLE #CustomSchemaTable	(
		[Name] nvarchar(255), 
		[Type] nvarchar(255)
	)

	SET @Exec = 'SELECT ''N7_HashedNonKeyColumns'' as [Name], ''nvarchar(32)'' as [Type] 
					UNION SELECT ''N7_HashedPKColumn'' as [Name], ''nvarchar(64)'' as [Type] ' 
					+ @CustomSchema
	IF @Debug = 1 PRINT '@Exec: ' + @Exec
	INSERT INTO #CustomSchemaTable
	EXEC sys.sp_executesql @Exec;	

	WITH RowSchema_cte as (
		SELECT  [COLUMN_NAME] AS [Name],
			QUOTENAME([COLUMN_NAME]) + ISNULL(' = CAST(' + QUOTENAME([COLUMN_NAME]) + ' AS ' + 
			CASE
				WHEN [CustomSchema].[Type] IN ('char', 'nchar', 'varchar', 'nvarchar', 'binary', 'varbinary') THEN CONCAT([CustomSchema].[Type], '(', CASE WHEN [CHARACTER_MAXIMUM_LENGTH] BETWEEN 0 AND 8000 THEN CAST([CHARACTER_MAXIMUM_LENGTH] AS nvarchar) ELSE 'max' END, ')')  --max is niet ondersteund door sql on-demand
				WHEN [CustomSchema].[Type] IN ('datetime2', 'datetimeoffset') THEN CONCAT([CustomSchema].[Type], '(', [DATETIME_PRECISION], ')')
				WHEN [CustomSchema].[Type] IN ('numeric', 'decimal') THEN CONCAT([CustomSchema].[Type], '(', [NUMERIC_PRECISION], ', ', [NUMERIC_SCALE], ')') 
				WHEN [CustomSchema].[Type] IN ('float') THEN CONCAT([CustomSchema].[Type], '(', [NUMERIC_PRECISION], ')') 
				ELSE [CustomSchema].[Type] --int, bigint, date, datetime, bit
			END + ')', '') AS [SelectSchema],
			QUOTENAME([COLUMN_NAME]) + ' ' +
			CASE
				WHEN [DATA_TYPE] IN ('char', 'nchar', 'varchar', 'nvarchar', 'binary', 'varbinary') THEN CONCAT([DATA_TYPE], '(', CASE WHEN [CHARACTER_MAXIMUM_LENGTH] BETWEEN 0 AND 8000 THEN CAST([CHARACTER_MAXIMUM_LENGTH] AS nvarchar) ELSE 'max' END, ')')  --max is niet ondersteund door sql on-demand
				WHEN [DATA_TYPE] IN ('datetime2', 'datetimeoffset') THEN CONCAT([DATA_TYPE], '(', [DATETIME_PRECISION], ')')
				WHEN [DATA_TYPE] IN ('numeric', 'decimal') THEN CONCAT([DATA_TYPE], '(', [NUMERIC_PRECISION], ', ', [NUMERIC_SCALE], ')') 
				WHEN [DATA_TYPE] IN ('float') THEN CONCAT([DATA_TYPE], '(', [NUMERIC_PRECISION], ')') 
				ELSE [DATA_TYPE] --int, bigint, date, datetime, bit
			END AS [WithSchema]
		FROM(
			SELECT *
			FROM  INFORMATION_SCHEMA.[COLUMNS]
			WHERE [TABLE_SCHEMA] = 'DeltaLake'
				AND [TABLE_NAME] = CONCAT(@TargetSchema, '_', @TargetTable)
				AND (';' + ISNULL(@DataStoreExcludeColumns, '') + ';' NOT LIKE '%;' + CASE WHEN ISNULL(@DataStoreIncludeColumns, '') != '' THEN ISNULL(@DataStoreExcludeColumns, '') ELSE [COLUMN_NAME] END + ';%' -- exclude
					OR ';' + ISNULL(@DataStoreIncludeColumns, '') + ';' LIKE '%;' + [COLUMN_NAME] + ';%' --include
					OR LEFT([COLUMN_NAME], 3) = 'N7_')
		)a
		LEFT JOIN #CustomSchemaTable AS [CustomSchema]
			ON [CustomSchema].[name] = [COLUMN_NAME]
	)

	SELECT @SelectSchema = [SelectSchema].[SelectSchema], 
		@WithSchema = [WithSchema].[WithSchema]
	FROM (
			SELECT CAST(STRING_AGG(CAST([SelectSchema] as NVARCHAR(MAX)), ', ') 
				WITHIN GROUP (ORDER BY CASE 
						WHEN [Name] = 'N7_RecordKey' THEN 'ZZZ1_' + [Name]
						WHEN [Name] = 'N7_RecordStartDate' THEN 'ZZZ2_' + [Name]
						WHEN [Name] = 'N7_RecordEndDate' THEN 'ZZZ3_' + [Name]
						WHEN [Name] = 'N7_RecordModifiedDate' THEN 'ZZZ4_' + [Name]
						WHEN [Name] LIKE 'N7_%' THEN 'ZZZ9_' + [Name] 
						ELSE '0' END ASC) as NVARCHAR(MAX)) AS [SelectSchema]
			FROM [RowSchema_cte]
			WHERE ';' + ISNULL(@DataStoreExcludeColumns, '') + ';' NOT LIKE '%;' + [Name] + ';%'
		) AS [SelectSchema]
	CROSS APPLY (
			SELECT CAST(STRING_AGG(CAST([WithSchema] as NVARCHAR(MAX)), ', ')
				WITHIN GROUP (ORDER BY CASE 
						WHEN [Name] = 'N7_RecordKey' THEN 'ZZZ1_' + [Name]
						WHEN [Name] = 'N7_RecordStartDate' THEN 'ZZZ2_' + [Name]
						WHEN [Name] = 'N7_RecordEndDate' THEN 'ZZZ3_' + [Name]
						WHEN [Name] = 'N7_RecordModifiedDate' THEN 'ZZZ4_' + [Name]
						WHEN [Name] LIKE 'N7_%' THEN 'ZZZ9_' + [Name] 
						ELSE '0' END ASC) as NVARCHAR(MAX)) AS [WithSchema]
			FROM [RowSchema_cte]
			WHERE ';' + ISNULL(@DataStoreExcludeColumns, '') + ';' NOT LIKE '%;' + [Name] + ';%'
		) AS [WithSchema]

	IF @SourceLayer = 'deltalake'
	BEGIN
		-- create historical view
		SET @Exec = 'CREATE OR ALTER VIEW [Historical].' + QUOTENAME(@TargetSchema + '_' + @TargetTable) + ' AS
					-- View created: ' + CONVERT(nvarchar(23), GETDATE(), 25) + ' 
					SELECT ' + @SelectSchema + '
					FROM
						OPENROWSET(
							BULK (' + @SourceFilePath + '),
							FORMAT=''DELTA''
						) WITH (
							' + @WithSchema + '
						) AS [result]'

		IF @Debug = 1 PRINT '@Exec: ' + @Exec
		EXEC sys.sp_executesql @Exec;

		-- create current view
		SET @Exec = 'CREATE OR ALTER VIEW [Current].' + QUOTENAME(@TargetSchema + '_' + @TargetTable) + ' AS
					-- View created: ' + CONVERT(nvarchar(23), GETDATE(), 25) + ' 
					SELECT ' + @SelectSchema + '
					FROM
						OPENROWSET(
							BULK (' + @SourceFilePath + '),
							FORMAT=''DELTA''
						) WITH (
							' + @WithSchema + '
						) AS [result]
					WHERE [N7_IsCurrent] = 1
						AND [N7_IsDeleted] = 0'

		IF @Debug = 1 PRINT '@Exec: ' + @Exec
		EXEC sys.sp_executesql @Exec;
	END

	IF @SourceLayer = 'raw'
	BEGIN
		-- create current view
		SET @Exec = 'CREATE OR ALTER VIEW [Current].' + QUOTENAME(@TargetSchema + '_' + @TargetTable) + ' AS
					-- View created: ' + CONVERT(nvarchar(23), GETDATE(), 25) + ' 
					SELECT ' + @SelectSchema + ',
						[N7_RecordStartDate] = TRY_CAST(CASE
													WHEN SUBSTRING([result].filename(), LEN([result].filename()) - 13, 1) = ''_''
													THEN LEFT(RIGHT([result].filename(), 26), 4) + ''-'' + 
															LEFT(RIGHT([result].filename(), 22), 2) + ''-'' + 
															LEFT(RIGHT([result].filename(), 20), 2) + '' '' + 
															LEFT(RIGHT([result].filename(), 18), 2) + '':'' + 
															LEFT(RIGHT([result].filename(), 16), 2) 
													ELSE LEFT(RIGHT([result].filename(), 20), 4) + ''-'' + 
															LEFT(RIGHT([result].filename(), 16), 2) + ''-'' + 
															LEFT(RIGHT([result].filename(), 14), 2) + '' '' + 
															LEFT(RIGHT([result].filename(), 12), 2) + '':'' + 
															LEFT(RIGHT([result].filename(), 10), 2)
												END AS datetime2(0)),
						[N7_RecordEndDate] = CAST (''99991231'' AS datetime2(0)),
						[N7_IsCurrent] = CAST(1 AS bit),
						[N7_IsDeleted] = CAST(0 AS bit)
					FROM
						OPENROWSET(
							BULK (' + @SourceFilePath + '),
							FORMAT=''PARQUET''
						) WITH (
							' + @WithSchema + '
						) AS [result]'

		IF @Debug = 1 PRINT '@Exec: ' + @Exec
		EXEC sys.sp_executesql @Exec;
	END

	-- drop temp view
	SET @Exec = 'DROP VIEW [DeltaLake].' + QUOTENAME(@TargetSchema + '_' + @TargetTable)

	IF @Debug = 1 PRINT '@Exec: ' + @Exec
	EXEC sys.sp_executesql @Exec;
END