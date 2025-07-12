CREATE EXTERNAL TABLE bronze.EClialias(
	idCliente int ,
	idAlias int ,
	fechaHoraAlta datetime NULL,
	anulado bit NULL,
	idTipoPersona nvarchar(100) NULL,
	apellidoPaterno nvarchar(100) NULL,
	apellidoMaterno nvarchar(100) NULL,
	nombres nvarchar(100) NULL,
	razonSocial nvarchar(100) NULL,
	fantasiaSocial nvarchar(100) NULL,
	idTipoContribuyente nvarchar(100) NULL,
	desTipoContribuyente nvarchar(100) NULL,
	idTipoIdentificador int NULL,
	desTipoIdentificador nvarchar(100) NULL,
	identificador nvarchar(100) NULL,
	vencimientoIdentificador datetime NULL,
	esExentoIibb bit NULL,
	fechaVencimientoExencionIibb datetime NULL,
	esInscriptoIibb bit NULL,
	numeroInscripcionIibb nvarchar(100) NULL,
	esAgentePercepcionIibb bit NULL,
	esConvenioMultilateral bit NULL,
	provinciasCm05 nvarchar(100) NULL,
	permisoVentaAlcohol bit NULL,
	vencimientoPermisoVentaAlcohol datetime NULL,
	esGranEmpresa bit NULL,
	esMipyme bit NULL
)
WITH (
    LOCATION = 'chess/source_files/EClialias.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)