CREATE EXTERNAL TABLE bronze.EPersCom(
	idSucursal int ,
	desSucursal nvarchar(100) NULL,
	idPersonal int ,
	desPersonal nvarchar(100) NULL,
	idFuerzaVentas int NULL,
	desFuerzaVentas nvarchar(100) NULL,
	cargo nvarchar(100) NULL,
	tipoVenta nvarchar(100) NULL,
	idPersonalSuperior int NULL,
	desPersonalSuperior nvarchar(100) NULL,
	domicilio nvarchar(100) NULL,
	telefono nvarchar(50) NULL,
	fechaNacimiento datetime NULL,
	usuarioSistema nvarchar(100) NULL,
	idTipoSegmento int NULL,
	desTipoSegmento nvarchar(100) NULL
)
WITH (
    LOCATION = 'chess/source_files/EPerscom.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)