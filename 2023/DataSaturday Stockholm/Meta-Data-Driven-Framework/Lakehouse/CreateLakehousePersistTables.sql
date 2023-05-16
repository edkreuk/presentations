CREATE OR ALTER PROCEDURE [Execution].[CreateLakehousePersistTables] (
	@SourceFilePath				nvarchar(4000)   --full path and filename (possible with wildcards to the source files)
	,@DestinationName			nvarchar(255)
)
AS

/**********************************************************************************************************
* Purpose:		Create Lakehouse views to persist
* 
* Revision Date/Time:
*  Erwin de Kreuk (InSpark) - Initial creation of SP
*
**********************************************************************************************************/
BEGIN

	/*
	DECLARE @SourceFilePath			nvarchar(4000)		= 'https://labseuwdvlmdloxgn01.dfs.core.windows.net/intermediate/Lakehouse/Dwh.Test.parquet/'
	DECLARE @DestinationName				nvarchar(255)		= 'Dwh.Test'
	-- */

	DECLARE @Exec						AS nvarchar(max)		= NULL
		,	@Debug						AS bit					= 1 -- Show debug output

	SET @SourceFilePath = '''' + @SourceFilePath + ''', ''' + @SourceFilePath + '/**''';

	-- create current view
	SET @Exec = 'CREATE OR ALTER VIEW ' + @DestinationName + ' AS
				SELECT *
				FROM
					OPENROWSET(
						BULK (' + @SourceFilePath + '),
						FORMAT=''PARQUET''
					) AS [result]'

	IF @Debug = 1 PRINT '@Exec: ' + @Exec
	EXEC sys.sp_executesql @Exec;
END