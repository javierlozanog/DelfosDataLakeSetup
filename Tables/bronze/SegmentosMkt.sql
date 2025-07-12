CREATE EXTERNAL TABLE bronze.SegmentosMkt(
	idSegmentoMkt int ,
	desSegmentoMkt nvarchar(100) NULL,
	compania bit NULL
)
WITH (
    LOCATION = 'chess/source_files/SegmentosMkt.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)