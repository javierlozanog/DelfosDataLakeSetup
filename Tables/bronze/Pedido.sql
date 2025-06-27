CREATE EXTERNAL TABLE bronze.Pedido(
	idPedido nvarchar(50) ,
	origen nvarchar(100) NULL,
	idUsuario nvarchar(50) NULL,
	idEmpresa int ,
	idSucursal int ,
	idFuerzaVentas int NULL,
	idDeposito int NULL,
	idFormaPago int NULL,
	idTipoDocumento nvarchar(50) NULL,
	idCliente int NULL,
	idAliasCliente int NULL,
	fechaEntrega nvarchar(50) NULL,
	idVendedor int NULL,
	idModoAtencion nvarchar(50) NULL,
	emitido bit NULL,
	fechaHoraEmision datetime NULL,
	modificado bit NULL,
	fechaHoraModificacion datetime NULL,
	eliminado bit NULL,
	IdReparto int NULL,
	Reparto nvarchar(100) NULL
)
WITH (
    LOCATION = 'chess/source_files/Pedido.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)