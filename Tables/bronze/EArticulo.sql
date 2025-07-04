CREATE EXTERNAL TABLE bronze.EArticulo(
	idArticulo int ,
	desArticulo nvarchar(100) NULL,
	descDetallada nvarchar(max) NULL,
	unidadesBulto int NULL,
	anulado bit NULL,
	fechaAlta datetime NULL,
	factorVenta int NULL,
	minimoVenta int NULL,
	pesable bit NULL,
	pesoCotaSuperior decimal(14, 4) NULL,
	pesoCotaInferior decimal(14, 4) NULL,
	esCombo bit NULL,
	detalleComboImp bit NULL,
	detalleComboInf bit NULL,
	exentoIva bit NULL,
	inafecto bit NULL,
	exonerado bit NULL,
	ivaDiferencial bit NULL,
	tasaIva decimal(14, 4) NULL,
	tasaInternos decimal(14, 4) NULL,
	internosBulto decimal(14, 4) NULL,
	tasaIibb decimal(14, 4) NULL,
	esAlcoholico bit NULL,
	visibleMobile bit NULL,
	esComodatable bit NULL,
	desCortaArticulo nvarchar(100) NULL,
	idPresentacionBulto nvarchar(100) NULL,
	desPresentacionBulto nvarchar(100) NULL,
	idPresentacionUnidad nvarchar(100) NULL,
	desPresentacionUnidad nvarchar(100) NULL,
	idUnidadMedida int NULL,
	desUnidadMedida nvarchar(100) NULL,
	valorUnidadMedida decimal(14, 4) NULL,
	idArticuloEstadistico int NULL,
	codBarraBulto nvarchar(100) NULL,
	codBarraUnidad nvarchar(100) NULL,
	tieneRetornables bit NULL,
	bultosPallet int NULL,
	pisosPallet int NULL,
	apilabilidad decimal(14, 4) NULL,
	pesoBulto decimal(14, 4) NULL,
	llevaFrescura bit NULL,
	diasBloqueo int NULL,
	politicaStock int NULL,
	diasVentana int NULL,
	esActivoFijo bit NULL,
	cantidadPuertas int NULL,
	unidadesFrente int NULL,
	litrosRepago decimal(14, 4) NULL,
	idArtUsado int NULL,
	aniosAmortizacion int NULL
)
WITH (
    LOCATION = 'chess/source_files/EArticulo.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)