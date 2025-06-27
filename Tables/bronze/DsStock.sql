CREATE EXTERNAL TABLE bronze.DsStock(
	fecha datetime NULL,
	idDeposito int ,
	idAlmacen int ,
	idArticulo int ,
	dsArticulo nvarchar(100) NULL,
	fecVtoLote datetime NULL,
	cantBultos int NULL,
	cantUnidades int NULL
)
WITH (
    LOCATION = 'chess/source_files/Dsstocks.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)