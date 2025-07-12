DECLARE @StartDate DATE
DECLARE @EndDate DATE
DECLARE @Date DATE
    
DECLARE @Year INT
DECLARE @Month INT
DECLARE @Day INT

DECLARE @SQL NVARCHAR(MAX) 
DECLARE @Version INT
DECLARE @TableName as VARCHAR(100)
DECLARE @dateFormat AS varchar(14) = FORMAT(getdate(),'yyyyMMddHHmmss')
DECLARE @folderName as VARCHAR(100)
SET @TableName = CONCAT('FactMascara',@dateFormat)
DECLARE @ErrorNum INT
DECLARE @ErrorMsg VARCHAR(500)
DROP TABLE IF EXISTS #Dates
CREATE TABLE #Dates (FechaFac DATE)
INSERT INTO #Dates EXEC helpers.spGetDatesToUpdate

-- INSERT NEW RECORDS
SET @StartDate = (SELECT MIN(FechaFac) FROM #Dates)
SET @EndDate = (SELECT MAX(FechaFac) FROM #Dates)
SET @Date = @StartDate
WHILE @Date <= @EndDate
BEGIN
	SET @Year = YEAR(@Date)
    SET @Month = MONTH(@Date)
    SET @Day = DAY(@Date)

	IF EXISTS (SELECT TOP 1 1
						FROM
							bronze.VentasResumen T1
						LEFT JOIN gold.VentasResumen T2 ON ISNULL(T2.Letra,'') = ISNULL(T1.letra,'')
												AND T2.IdDocumento = T1.iddocumento
												AND T2.IdEmpresa = T1.idempresa
												AND T2.idSucursal = T1.idSucursal
												AND T2.NroDoc = T1.nrodoc
												AND T2.Serie = T1.serie
						WHERE
							CONVERT(datetime2,T1.fechaComprobate,103) = @Date
							AND T2.fechaComprobate IS NULL
	)
	BEGIN	
		SET @Version = 1
		SET @folderName = CONCAT('/chess/parquet_files/ventasresumen/Year=',CAST(@Year as VARCHAR(4)), '/Month=', CAST(@Month as VARCHAR(2)),'/Day=',CAST(@Day as VARCHAR(2)) ,'/Ver=',CAST(@Version AS VARCHAR(10)),'/',@dateFormat,'/')

		SET @SQL = '
				CREATE EXTERNAL TABLE '+ @TableName +  
				' WITH (
					LOCATION = ''' + @folderName +''',
					DATA_SOURCE = eds_delfos,  
					FILE_FORMAT = eff_delfos_parquet
				)  
				AS
				SELECT
					T1.idEmpresa,
					T1.dsEmpresa,
					T1.idDocumento,
					T1.dsDocumento,
					T1.letra,
					T1.serie,
					T1.nrodoc,
					T1.pickup,
					T1.anulado,
					T1.idMovComercial,
					T1.dsMovComercial,
					T1.idRechazo,
					T1.dsRechazo,
					T1.fechaComprobate,
					T1.fechaAnulacion,
					T1.fechaAlta,
					T1.usuarioAlta,
					T1.fechaVencimiento,
					T1.fechaEntrega,
					T1.idSucursal,
					T1.dsSucursal,
					T1.idFuerzaVentas,
					T1.dsFuerzaVentas,
					T1.idDeposito,
					T1.dsDeposito,
					T1.idVendedor,
					T1.dsVendedor,
					T1.idSupervisor,
					T1.dsSupervisor,
					T1.idGerente,
					T1.dsGerente,
					T1.tipoConstribuyente,
					T1.dsTipoConstribuyente,
					T1.idTipoPago,
					T1.dsTipoPago,
					T1.fechaPago,
					T1.idPedido,
					T1.fechaPedido,
					T1.origen,
					T1.idorigen,
					T1.planillaCarga,
					T1.idFleteroCarga,
					T1.dsFleteroCarga,
					T1.idLiquidacion,
					T1.fechaLiquidacion,
					T1.idCaja,
					T1.fechaCaja,
					T1.cajero,
					T1.idCliente,
					T1.nombreCliente,
					T1.domicilioCliente,
					T1.codigoPostal,
					T1.dsLocalidad,
					T1.idProvincia,
					T1.dsProvincia,
					T1.idNegocio,
					T1.dsNegocio,
					T1.idAgrupacion,
					T1.dsAgrupacion,
					T1.idArea,
					T1.dsArea,
					T1.idSegmentoMkt,
					T1.dsSegmentoMkt,
					T1.idCanalMkt,
					T1.dsCanalMkt,
					T1.idSubcanalMkt,
					T1.dsSubcanalMKT,
					T1.idLinea,
					T1.idArticulo,
					T1.dsArticulo,
					T1.idConcepto,
					T1.dsConcepto,
					T1.esCombo,
					T1.idCombo,
					T1.idArticuloEstadistico,
					T1.dsArticuloEstadistico,
					T1.presentacionArticulo,
					T1.cantidadPorPallets,
					T1.peso,
					T1.fechaAsientoContable,
					T1.nroAsientoContable,
					T1.nroPlanContable,
					T1.codCuentaContable,
					T1.idCentroCosto,
					T1.dsCuentaContable,
					T1.cantidadSolicitada,
					T1.unidadesSolicitadas,
					T1.cantidadesCorCargo,
					T1.cantidadesSinCargo,
					T1.cantidadesTotal,
					T1.pesoTotal,
					T1.cantidadesRechazo,
					T1.unimedcargo,
					T1.unimedscargo,
					T1.unimedtotal,
					T1.precioUnitarioBruto,
					T1.bonificacion,
					T1.precioUnitarioNeto,
					T1.subtotalBruto,
					T1.subtotalBonificado,
					T1.subtotalNeto,
					T1.iva21,
					T1.iva27,
					T1.per3337,
					T1.iva2,
					T1.percepcion212,
					T1.percepcioniibb,
					T1.internos,
					T1.subtotalFinal,
					T1.tradespendg,
					T1.tradespends,
					T1.tradespendb,
					T1.tradespendi,
					T1.tradespendp,
					T1.tradespendt,
					T1.totradspend,
					T1.acciones,
					T1.persiibbd,
					T1.persiibbr,
					T1.numerosserie,
					T1.numerosactivo,
					T1.cuentayorden,
					T1.codprovcyo,
					T1.descrip,
					T1.nrorendcyo,
					T1.idTipoCambio,
					T1.dsTipoCambio,
					T1.cfdiEmitido,
					T1.regimenFiscal,
					T1.informado,
					T1.firmadigital,
					T1.proveedor,
					T1.fvigpcompra,
					T1.preciocomprabr,
					T1.preciocomprant,
					T1.lineaCredito,
					T1.tipocambio,
					T1.motivocambio,
					T1.descmotcambio,
					T1.numeracionFiscal,
					T1.codproviibb,
					T1.TipoId,
					T1.DescripcionId,
					T1.Identificador,'
					+ CAST(@Version AS VARCHAR(10)) + ' AS Ver ' +
			' FROM 
					bronze.VentasResumen T1
					LEFT JOIN gold.VentasResumen T2 ON ISNULL(T2.Letra,'''') = ISNULL(T1.letra,'''')
												AND T2.IdDocumento = T1.iddocumento
												AND T2.IdEmpresa = T1.idempresa
												AND T2.idSucursal = T1.idSucursal
												AND T2.NroDoc = T1.nrodoc
												AND T2.Serie = T1.serie
						WHERE
							CONVERT(datetime2,T1.fechaComprobate,103) = CONVERT(datetime2,''' + CAST(@Date AS varchar(10)) + ''') ' + '
							AND T2.fechaComprobate IS NULL
			'
			EXEC(@SQL)

			EXEC helpers.DropExternalTable @TableName;

	END
	
	SET @Date = DATEADD(day,1,@Date)
END