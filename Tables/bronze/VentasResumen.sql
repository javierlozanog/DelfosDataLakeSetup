CREATE EXTERNAL TABLE bronze.VentasResumen (
	idEmpresa int,
	dsEmpresa nvarchar(100) NULL,
	idDocumento nvarchar(10) NULL,
	dsDocumento nvarchar(50) NULL,
	letra nvarchar(2),
	serie int,
	nrodoc int,
	pickup nvarchar(2) NULL,
	anulado nvarchar(2) NULL,
	idMovComercial int NULL,
	dsMovComercial nvarchar(100) NULL,
	idRechazo int NULL,
	dsRechazo nvarchar(100) NULL,
	fechaComprobate datetime,
	fechaAnulacion date NULL,
	fechaAlta date NULL,
	usuarioAlta nvarchar(100) NULL,
	fechaVencimiento date NULL,
	fechaEntrega date NULL,
	idSucursal int,
	dsSucursal nvarchar(100) NULL,
	idFuerzaVentas int NULL,
	dsFuerzaVentas nvarchar(100) NULL,
	idDeposito int NULL,
	dsDeposito nvarchar(100) NULL,
	idVendedor int NULL,
	dsVendedor nvarchar(100) NULL,
	idSupervisor int NULL,
	dsSupervisor nvarchar(100) NULL,
	idGerente int NULL,
	dsGerente nvarchar(100) NULL,
	tipoConstribuyente nvarchar(100) NULL,
	dsTipoConstribuyente nvarchar(100) NULL,
	idTipoPago int NULL,
	dsTipoPago nvarchar(100) NULL,
	fechaPago date NULL,
	idPedido int NULL,
	fechaPedido date NULL,
	origen nvarchar(100) NULL,
	idorigen nvarchar(100) NULL,
	planillaCarga nvarchar(100) NULL,
	idFleteroCarga int NULL,
	dsFleteroCarga nvarchar(100) NULL,
	idLiquidacion int NULL,
	fechaLiquidacion date NULL,
	idCaja int NULL,
	fechaCaja date NULL,
	cajero nvarchar(100) NULL,
	idCliente int NULL,
	nombreCliente nvarchar(100) NULL,
	domicilioCliente nvarchar(500) NULL,
	codigoPostal int NULL,
	dsLocalidad nvarchar(100) NULL,
	idProvincia nvarchar(100) NULL,
	dsProvincia nvarchar(100) NULL,
	idNegocio int NULL,
	dsNegocio nvarchar(100) NULL,
	idAgrupacion int NULL,
	dsAgrupacion nvarchar(100) NULL,
	idArea int NULL,
	dsArea nvarchar(100) NULL,
	idSegmentoMkt int NULL,
	dsSegmentoMkt nvarchar(100) NULL,
	idCanalMkt int NULL,
	dsCanalMkt nvarchar(100) NULL,
	idSubcanalMkt int NULL,
	dsSubcanalMKT nvarchar(100) NULL,
	idLinea int,
	idArticulo int,
	dsArticulo nvarchar(100) NULL,
	idConcepto int NULL,
	dsConcepto nvarchar(100) NULL,
	esCombo nvarchar(100) NULL,
	idCombo int NULL,
	idArticuloEstadistico int NULL,
	dsArticuloEstadistico nvarchar(100) NULL,
	presentacionArticulo int NULL,
	cantidadPorPallets int NULL,
	peso decimal(14, 4) NULL,
	fechaAsientoContable date NULL,
	nroAsientoContable int NULL,
	nroPlanContable int NULL,
	codCuentaContable int NULL,
	idCentroCosto int NULL,
	dsCuentaContable nvarchar(100) NULL,
	cantidadSolicitada int NULL,
	unidadesSolicitadas int NULL,
	cantidadesCorCargo decimal(14, 4) NULL,
	cantidadesSinCargo decimal(14, 4) NULL,
	cantidadesTotal decimal(14, 4) NULL,
	pesoTotal decimal(14, 4) NULL,
	cantidadesRechazo decimal(14, 4) NULL,
	unimedcargo decimal(14, 4) NULL,
	unimedscargo decimal(14, 4) NULL,
	unimedtotal decimal(14, 4) NULL,
	precioUnitarioBruto decimal(14, 4) NULL,
	bonificacion decimal(14, 4) NULL,
	precioUnitarioNeto decimal(14, 4) NULL,
	subtotalBruto decimal(14, 4) NULL,
	subtotalBonificado decimal(14, 4) NULL,
	subtotalNeto decimal(14, 4) NULL,
	iva21 decimal(14, 4) NULL,
	iva27 decimal(14, 4) NULL,
	per3337 decimal(14, 4) NULL,
	iva2 decimal(14, 4) NULL,
	percepcion212 decimal(14, 4) NULL,
	percepcioniibb decimal(14, 4) NULL,
	internos decimal(14, 4) NULL,
	subtotalFinal decimal(14, 4) NULL,
	tradespendg decimal(14, 4) NULL,
	tradespends decimal(14, 4) NULL,
	tradespendb decimal(14, 4) NULL,
	tradespendi decimal(14, 4) NULL,
	tradespendp decimal(14, 4) NULL,
	tradespendt decimal(14, 4) NULL,
	totradspend decimal(14, 4) NULL,
	acciones nvarchar(100) NULL,
	persiibbd nvarchar(100) NULL,
	persiibbr nvarchar(100) NULL,
	numerosserie nvarchar(100) NULL,
	numerosactivo nvarchar(100) NULL,
	cuentayorden nvarchar(100) NULL,
	codprovcyo int NULL,
	descrip nvarchar(100) NULL,
	nrorendcyo int NULL,
	idTipoCambio int NULL,
	dsTipoCambio nvarchar(100) NULL,
	cfdiEmitido nvarchar(100) NULL,
	regimenFiscal nvarchar(100) NULL,
	informado nvarchar(100) NULL,
	firmadigital nvarchar(100) NULL,
	proveedor nvarchar(100) NULL,
	fvigpcompra nvarchar(100) NULL,
	preciocomprabr decimal(14, 4) NULL,
	preciocomprant decimal(14, 4) NULL,
	lineaCredito nvarchar(100) NULL,
	tipocambio nvarchar(100) NULL,
	motivocambio int NULL,
	descmotcambio nvarchar(100) NULL,
	numeracionFiscal nvarchar(100) NULL,
	codproviibb nvarchar(100) NULL,
	TipoId int NULL,
	DescripcionId nvarchar(100) NULL,
	Identificador nvarchar(100) NULL
)
WITH (
    LOCATION = 'chess/source_files/VentasResumen.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)
--exec ldw.UpdateFactMascara