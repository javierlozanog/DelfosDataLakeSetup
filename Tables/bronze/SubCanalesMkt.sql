CREATE EXTERNAL TABLE bronze.SubCanalesMkt(
	idSubcanalMkt int ,
	desSubcanalMkt nvarchar(100) NULL,
	idCanalMkt int NULL,
	compania bit NULL
)
WITH (
    LOCATION = 'chess/source_files/SubCanalesMkt.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)