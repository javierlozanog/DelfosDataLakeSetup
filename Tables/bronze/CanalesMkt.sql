CREATE EXTERNAL TABLE bronze.CanalesMkt(
	idCanalMkt int ,
	desCanalMkt nvarchar(100) NULL,
	idSegmentoMkt int NULL,
	compania bit NULL
)
WITH (
    LOCATION = 'chess/source_files/CanalesMkt.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)