CREATE OR ALTER   PROCEDURE [silver].[Articulo_Update]
AS
BEGIN
	DECLARE @Sql VARCHAR(MAX)
	DECLARE @Version INT = 1
	DECLARE @TableName as VARCHAR(100)
	DECLARE @dateFormat AS varchar(12) = FORMAT(getdate(),'yyyyMMddhhmm')
	SET @TableName = CONCAT('articulo',@dateFormat)

	IF EXISTS ( SELECT 
					idArticulo
					,desArticulo
					,anulado
					,unidadesBulto
					,pesable
					,esAlcoholico
					,esComodatable
					,idPresentacionBulto
					,valorUnidadMedida
					,idArticuloEstadistico
				FROM 
					bronze.EArticulo
				EXCEPT
				SELECT 
					idArticulo
					,desArticulo
					,anulado
					,unidadesBulto
					,pesable
					,esAlcoholico
					,esComodatable
					,idPresentacionBulto
					,valorUnidadMedida
					,idArticuloEstadistico
				FROM 
					gold.Articulo
    )
	BEGIN
		SET @Version = ISNULL(
							(SELECT
								MAX(result.filepath(1))
							FROM
								OPENROWSET(
									BULK 'chess/parquet_files/articulo/Ver=*/*.parquet',
									DATA_SOURCE='eds_delfos',
									FORMAT='PARQUET'
								) AS result )
							,0) + 1 
        
		DECLARE @folderName as VARCHAR(100) = CONCAT('/chess/parquet_files/articulo/Ver=',@Version)

		SET @SQL = 
				'CREATE EXTERNAL TABLE '+ @TableName +   
				' WITH (
						LOCATION = ''' + @folderName +''',
						DATA_SOURCE = eds_delfos,  
						FILE_FORMAT = eff_delfos_parquet
					)  
					AS
						SELECT 
							*,' 
							+  CAST(@Version AS VARCHAR(10)) + ' AS Ver' + 
						' FROM bronze.EArticulo
						WHERE 
							CHECKSUM(CONCAT(idArticulo,desArticulo,anulado,unidadesBulto,pesable,esAlcoholico,esComodatable,idPresentacionBulto,valorUnidadMedida,idArticuloEstadistico)) NOT IN
                            (
                                SELECT
                                    CHECKSUM(CONCAT(idArticulo,desArticulo,anulado,unidadesBulto,pesable,esAlcoholico,esComodatable,idPresentacionBulto,valorUnidadMedida,idArticuloEstadistico))
                                FROM
                                    gold.Articulo
                            )
				'
		EXEC (@SQL)
		EXEC helpers.DropExternalTable @TableName;

	END
END