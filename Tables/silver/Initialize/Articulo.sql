DECLARE @Sql VARCHAR(MAX)
DECLARE @Version INT = 0
DECLARE @dateFormat AS varchar(14) = '00000000000000'
DECLARE @folderName as VARCHAR(100) = CONCAT('/chess/parquet_files/articulo/Ver=0/',@dateFormat,'/')

SET @SQL = '
			CREATE EXTERNAL TABLE Articulo#NA
			WITH (
				LOCATION = '''+ @folderName + ''',
				DATA_SOURCE = eds_delfos,  
				FILE_FORMAT = eff_delfos_parquet
			)
			AS
			SELECT 
				CAST(0 AS int) AS idArticulo
				,CAST(''N/A'' AS VARCHAR(100)) AS desArticulo
				,CAST(''N/A'' AS VARCHAR(100)) AS descDetallada
				,CAST(0 AS int) AS unidadesBulto
				,CAST(0 AS bit) AS anulado
				,CAST(NULL AS datetime) AS fechaAlta
				,CAST(0 AS int) AS factorVenta
				,CAST(0 AS int) AS minimoVenta
				,CAST(0 AS bit) AS pesable
				,CAST(0 AS DECIMAL(14,4)) AS pesoCotaSuperior
				,CAST(0 AS DECIMAL(14,4)) AS pesoCotaInferior
				,CAST(0 AS bit) AS esCombo
				,CAST(0 AS bit) AS detalleComboImp
				,CAST(0 AS bit) AS detalleComboInf
				,CAST(0 AS bit) AS exentoIva
				,CAST(0 AS bit) AS inafecto
				,CAST(0 AS bit) AS exonerado
				,CAST(0 AS bit) AS ivaDiferencial
				,CAST(0 AS DECIMAL(14,4)) AS tasaIva
				,CAST(0 AS DECIMAL(14,4)) AS tasaInternos
				,CAST(0 AS DECIMAL(14,4)) AS internosBulto
				,CAST(0 AS DECIMAL(14,4)) AS tasaIibb
				,CAST(0 AS bit) AS esAlcoholico
				,CAST(0 AS bit) AS visibleMobile
				,CAST(0 AS bit) AS esComodatable
				,CAST(''N/A'' AS VARCHAR(100)) AS desCortaArticulo
				,CAST(''N/A'' AS VARCHAR(100)) AS idPresentacionBulto
				,CAST(''N/A'' AS VARCHAR(100)) AS desPresentacionBulto
				,CAST(''N/A'' AS VARCHAR(100)) AS idPresentacionUnidad
				,CAST(''N/A'' AS VARCHAR(100)) AS desPresentacionUnidad
				,CAST(0 AS int) AS idUnidadMedida
				,CAST(''N/A'' AS VARCHAR(100)) AS desUnidadMedida
				,CAST(0 AS DECIMAL(14,4)) AS valorUnidadMedida
				,CAST(0 AS int) AS idArticuloEstadistico
				,CAST(''N/A'' AS VARCHAR(100)) AS codBarraBulto
				,CAST(''N/A'' AS VARCHAR(100)) AS codBarraUnidad
				,CAST(0 AS bit) AS tieneRetornables
				,CAST(0 AS int) AS bultosPallet
				,CAST(0 AS int) AS pisosPallet
				,CAST(0 AS DECIMAL(14,4)) AS apilabilidad
				,CAST(0 AS DECIMAL(14,4)) AS pesoBulto
				,CAST(0 AS bit) AS llevaFrescura
				,CAST(0 AS int) AS diasBloqueo
				,CAST(0 AS int) AS politicaStock
				,CAST(0 AS int) AS diasVentana
				,CAST(0 AS bit) AS esActivoFijo
				,CAST(0 AS int) AS cantidadPuertas
				,CAST(0 AS int) AS unidadesFrente
				,CAST(0 AS DECIMAL(14,4)) AS litrosRepago
				,CAST(0 AS int) AS idArtUsado
				,CAST(0 AS int) AS aniosAmortizacion
				,CAST(0 AS INT) as Ver
			'
EXEC (@SQL)

DROP EXTERNAL TABLE Articulo#NA

CREATE EXTERNAL TABLE silver.EArticulo(
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
	aniosAmortizacion int NULL,
    Ver INT
)
WITH (
    LOCATION = 'chess/parquet_files/articulo/Ver=*/*/*.parquet',
    DATA_SOURCE = eds_delfos,  
    FILE_FORMAT = eff_delfos_parquet
)