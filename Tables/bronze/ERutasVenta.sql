CREATE EXTERNAL TABLE bronze.ERutasVenta(
	idSucursal int ,
	desSucursal nvarchar(100) NULL,
	idFuerzaVentas int ,
	desFuerzaVentas nvarchar(100) NULL,
	idModoAtencion nvarchar(50) ,
	desModoAtencion nvarchar(100) NULL,
	idRuta int ,
	desRuta nvarchar(100) NULL,
	fechaDesde nvarchar(50) NULL,
	fechaHasta nvarchar(50) NULL,
	anulado bit NULL,
	idPersonal int ,
	desPersonal nvarchar(100) NULL,
	periodicidadVisita int NULL,
	semanaVisita int NULL,
	diasVisita nvarchar(50) NULL,
	periodicidadEntrega int NULL,
	semanaEntrega int NULL,
	diasEntrega nvarchar(50) NULL,
	IdRutaAuto uniqueidentifier 
)
WITH (
    LOCATION = 'chess/source_files/ERutasVenta.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)