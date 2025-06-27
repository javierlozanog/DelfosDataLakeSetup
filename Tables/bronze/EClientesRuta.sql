CREATE EXTERNAL TABLE bronze.EClientesRuta(
	idCliente int ,
	razonSocial nvarchar(100) NULL,
	intercalacionVisita int NULL,
	intercalacionEntrega int NULL,
	IdRutaAuto uniqueidentifier ,
	IdClienteRutaAuto uniqueidentifier 
)
WITH (
    LOCATION = 'chess/source_files/EClientesRuta.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)