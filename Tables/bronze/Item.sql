CREATE EXTERNAL TABLE bronze.Item(
	idPedido nvarchar(50) ,
	idLineaDetalle int ,
	idMotivoCambio int NULL,
	idArticulo int NULL,
	cantBultos int NULL,
	cantUnidades int NULL,
	pesoKilos decimal(14, 4) NULL,
	precioUnitario decimal(14, 4) NULL,
	bonificacion decimal(14, 4) NULL,
	anulado bit NULL
)
WITH (
    LOCATION = 'chess/source_files/Item.csv',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_csv
)