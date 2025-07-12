DECLARE @Sql VARCHAR(MAX)
DECLARE @Version INT = 0
DECLARE @dateFormat AS varchar(14) = '00000000000000'
DECLARE @folderName as VARCHAR(100) = CONCAT('/chess/parquet_files/agrupacion/Ver=0/',@dateFormat,'/')

SET @SQL = '
			CREATE EXTERNAL TABLE EAgrupacione#NA
			WITH (
				LOCATION = '''+ @folderName + ''',
				DATA_SOURCE = eds_delfos,  
				FILE_FORMAT = eff_delfos_parquet
			)
			AS
			SELECT
				CAST( NULL AS nvarchar(100)) AS idFormaAgrupar
				, CAST( NULL AS nvarchar(100)) AS desFormaAgrupar
				, CAST( NULL AS int) AS idArticulo
				, CAST( NULL AS nvarchar(100)) AS idAgrupacion
				, CAST( NULL AS nvarchar(100)) AS desAgrupacion
				, CAST( 0 AS int) AS Ver
			'
EXEC (@SQL)

DROP EXTERNAL TABLE EAgrupacione#NA

CREATE EXTERNAL TABLE [silver].[EAgrupacione]
(
	[idFormaAgrupar] [nvarchar](100),
	[desFormaAgrupar] [nvarchar](100),
	[idArticulo] [int],
	[idAgrupacion] [nvarchar](100),
	[desAgrupacion] [nvarchar](100),
	Ver INT
)
WITH (
    LOCATION = 'chess/parquet_files/agrupacion/Ver=*/*/*.parquet',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_parquet
)