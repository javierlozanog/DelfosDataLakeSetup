DECLARE @Sql VARCHAR(MAX)
DECLARE @Version INT = 1
DECLARE @dateFormat AS varchar(12) = FORMAT(getdate(),'yyyyMMddhhmm')
DECLARE @TableName as VARCHAR(100)
SET @TableName = CONCAT('Articulo',@dateFormat)

IF EXISTS ( SELECT TOP 1 1
            FROM
                bronze.EArticulo T1
            LEFT JOIN gold.Articulo T2 ON T2.IdArticulo = T1.IdArticulo
			WHERE T2.IdArticulo IS NULL
)
BEGIN
	DECLARE @folderName as VARCHAR(100) = CONCAT('/chess/parquet_files/articulo/Ver=',@Version,'/',@dateFormat,'/')

	SET @SQL = 
			'CREATE EXTERNAL TABLE '+ @TableName +   
			' WITH (
					LOCATION = ''' + @folderName +''',
					DATA_SOURCE = eds_delfos,  
					FILE_FORMAT = eff_delfos_parquet
				)  
				AS
					SELECT 
						T1.*,' 
						+  CAST(@Version AS VARCHAR(10)) + ' AS Ver' + ' FROM bronze.EArticulo  T1
					LEFT JOIN gold.Articulo T2 ON T2.IdArticulo = T1.IdArticulo
					WHERE T2.IdArticulo IS NULL
			'
	EXEC (@SQL)
		

	EXEC helpers.DropExternalTable @TableName;
END
