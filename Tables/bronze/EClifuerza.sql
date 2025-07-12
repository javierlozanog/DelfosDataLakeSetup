CREATE EXTERNAL TABLE bronze.EClifuerza(
	idSucursal int ,
	idCliente int ,
	idFuerzaVentas int ,
	desFuerzaVenta nvarchar(100) NULL,
	idModoAtencion nvarchar(100) NULL,
	desModoAtencion nvarchar(100) NULL,
	fechaInicioFuerza nvarchar(100) ,
	fechaFinFuerza nvarchar(100) NULL,
	idRuta int ,
	fechaRutaVenta nvarchar(100) NULL,
	anulado bit NULL,
	periodicidadVisita int NULL,
	semanaVisita int NULL,
	diasVisita nvarchar(100) NULL,
	intercalacionVisita int NULL,
	perioricidadEntrega int NULL,
	semanaEntrega int NULL,
	diasEntrega nvarchar(100) NULL,
	intercalacionEntrega int NULL,
	Horarios nvarchar(100) NULL
)
WITH (
    LOCATION = 'chess/source_files/EClifuerza.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)