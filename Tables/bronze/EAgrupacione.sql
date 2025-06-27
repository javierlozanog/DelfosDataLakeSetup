CREATE EXTERNAL TABLE bronze.EAgrupacione(
	idFormaAgrupar nvarchar(100) ,
	desFormaAgrupar nvarchar(100) NULL,
	idArticulo int ,
	idAgrupacion nvarchar(100) ,
	desAgrupacion nvarchar(100) NULL
)
WITH (
    LOCATION = 'chess/source_files/EAgrupacione.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)