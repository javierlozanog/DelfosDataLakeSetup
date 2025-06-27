BEGIN --AgrupacionesArt
    CREATE EXTERNAL TABLE stg.agrupacionesart (
            IdFormaAgrupar varchar(100) NULL,
	        IdAgrupacion varchar(100) NULL,
	        DsAgrupacion varchar(100) NULL,
	        DsAgrupCorta varchar(30) NULL
        ) WITH (
            LOCATION = '/chess/fonpell/agrupacionesart.csv',
            DATA_SOURCE = eds_openstorage_delfosldw,  
            FILE_FORMAT = eff_csv_source
        )

    CREATE EXTERNAL TABLE DimAgrupacionesArt#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimagrupacionesart/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('N/A' AS VARCHAR(100)) AS IdFormaAgrupar
        ,CAST('N/A' AS VARCHAR(100)) AS IdAgrupacion
        ,CAST('N/A' AS VARCHAR(100)) AS DsAgrupacion
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimAgrupacionesArt#NA

    CREATE EXTERNAL TABLE ldw.DimAgrupacionesArt (
	                IdFormaAgrupar varchar(100) NULL,
	                IdAgrupacion varchar(100) NULL,
	                DsAgrupacion varchar(100) NULL,
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimagrupacionesart/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )

	exec ldw.LoadDimAgrupacionesArt
END
BEGIN --AGRUPAS
	CREATE EXTERNAL TABLE stg.agrupas (
		codagru int,
		descagru varchar(100)
	)
	WITH (
		LOCATION = 'chess/agrupas.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)

	CREATE EXTERNAL TABLE DimAgrupas#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimagrupas/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('0' AS int) AS CodAgru
        ,CAST('N/A' AS VARCHAR(100)) AS DescAgru
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimAgrupas#NA

	CREATE EXTERNAL TABLE ldw.DimAgrupas (
	                CodAgru INT,
	                DescAgru varchar(100),
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimagrupas/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadDimAgrupas
END
BEGIN --AREAS
	CREATE EXTERNAL TABLE stg.areas (
			codarea int,
			descarea varchar(100)
		)
		WITH (
			LOCATION = 'chess/areas.csv',
			DATA_SOURCE = eds_openstorage_delfosldw,  
			FILE_FORMAT = eff_csv_source
		)
	
	CREATE EXTERNAL TABLE DimAreas#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimareas/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('0' AS int) AS CodArea
        ,CAST('N/A' AS VARCHAR(100)) AS DescArea
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimAreas#NA

	CREATE EXTERNAL TABLE ldw.DimAreas (
	                CodArea INT,
	                DescArea varchar(100),
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimareas/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadDimAreas
END
BEGIN-- Articulo
    CREATE EXTERNAL TABLE stg.articulo (
	    CodArt INT,
	    Descrip VARCHAR(100),
	    Resto INT,
	    Peso DECIMAL(6,2),
	    IvaDif BIT,
	    Valor DECIMAL(8,4),
	    Margen DECIMAL(6,2),
	    Anulado bit
    )
    WITH (
	    LOCATION = 'chess/articulo.csv',
	    DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
    )

    CREATE EXTERNAL TABLE DimArticulo#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimarticulo/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST(0 AS int) AS CodArt,
        CAST('N/A' AS VARCHAR(100)) AS Descrip,
        CAST(0 AS int) AS Resto,
        CAST(0 AS DECIMAL(6,2)) AS Peso,
        CAST(0 AS bit) AS IvaDif,
        CAST(0 AS DECIMAL(8,4)) AS Valor,
        CAST(0 AS DECIMAL(6,2)) AS Margen,
        CAST(0 AS bit) AS Anulado,
        CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimArticulo#NA

    CREATE EXTERNAL TABLE ldw.DimArticulo (
	                CodArt INT,
	                Descrip VARCHAR(100),
	                Resto INT,
	                Peso DECIMAL(6,2),
	                IvaDif BIT,
	                Valor DECIMAL(8,4),
	                Margen DECIMAL(6,2),
	                Anulado bit,
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimarticulo/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )

	EXEC ldw.LoadDimArticulo
END
BEGIN --CANALES
	CREATE EXTERNAL TABLE stg.canales (
		codcanal int,
		descrip varchar(100)
	)
	WITH (
		LOCATION = 'chess/canales.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)
	CREATE EXTERNAL TABLE DimCanales#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimcanales/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('0' AS int) AS CodCanal
        ,CAST('N/A' AS VARCHAR(100)) AS Descrip
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimCanales#NA

	CREATE EXTERNAL TABLE ldw.DimCanales (
	                CodCanal INT,
	                Descrip varchar(100),
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimcanales/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadDimCanales
END
BEGIN --CLIENTES
	CREATE EXTERNAL TABLE stg.clientes (
	    idcliente int,
		nomcli varchar(100),
		codagru int,
		codarea int,
		codcanal int,
		codnego int,
		codprovi varchar(2),
		idSucur int,
		idlocalidad int,
		codpost int,
		fecanu varchar(20)
	)
    WITH (
	    LOCATION = 'chess/clientes.csv',
	    DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
    )

	CREATE EXTERNAL TABLE DimCliente#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimcliente/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    ) 
	AS
    SELECT 
        CAST(0 AS int) AS IdCliente,
        CAST('N/A' AS VARCHAR(100)) AS Nomcli,
        CAST(0 AS int) AS CodAgru,
        CAST(0 AS int) AS CodArea,
		CAST(0 AS int) AS CodCanal,
		CAST(0 AS int) AS CodNego,
		CAST(0 AS varchar(2)) AS CodProvi,
		CAST(0 AS int) AS IdSucur,
		CAST(0 AS int) AS IdLocalidad,
		CAST(0 AS int) AS CodPost,
		CAST(null as datetime2) AS FecAnu,
        CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimCliente#NA

	CREATE EXTERNAL TABLE ldw.DimCliente (
	                IdCliente INT,
	                NomCli VARCHAR(100),
	                CodAgru INT,
	                CodArea INT,
					CodCanal INT,
					CodNego INT,
					CodProvi VARCHAR(2),
					IdSucur INT,
					IdLocalidad INT,
					CodPost INT,
					FecAnu DATETIME2,
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimcliente/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadClientes
END
BEGIN --CLIFUERZA
	CREATE EXTERNAL TABLE stg.clifuerza (
		idfuerzaventas int,
		idcliente int,
		idmodoatencion varchar(5),
		fecclifuerzadesde varchar(20),
		fecclifuerzahasta varchar(20),
		idsucur int,
		ruta int,
		anulado bit,
		fecanu varchar(20),
		diasvis varchar(100)
	)
	WITH (
		LOCATION = 'chess/fonpell/clifuerza.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)

	CREATE EXTERNAL TABLE CliFuerza#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimclifuerza/IdSucur=0/Ruta=0/IdFuerzaVentas=0/IdModoAtencion=NA/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST(0 AS INT) AS IdFuerzaVentas,
        CAST(0 AS INT) AS IdCliente,
		CAST('NA' AS VARCHAR(5)) AS IdModoAtencion,
		CAST('N/A' AS VARCHAR(20)) AS FecCliFuerzaDesde,
		CAST('N/A' AS VARCHAR(20)) AS FecCliFuerzaHasta,
		CAST(0 AS INT) AS IdSucur,
		CAST(0 AS INT) AS Ruta,
		CAST(0 AS bit) AS Anulado,
		CAST('N/A' AS VARCHAR(20)) AS FecAnulado,
		CAST('N/A' AS VARCHAR(100)) AS DiasVis,
        CAST(0 AS INT) as Ver


END
BEGIN --DEPARTAMENTOS
	CREATE EXTERNAL TABLE stg.departamentos (
		dsdepartamento varchar(100),
		idpais varchar(100),
		iddepartamento int,
		idprovincia int
	)
	WITH (
		LOCATION = 'chess/departamentos.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)
	CREATE EXTERNAL TABLE DimDepartamentos#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimdepartamentos/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('N/A' AS VARCHAR(100)) AS DsDepartamento
		,CAST('N/A' AS VARCHAR(100)) AS IdPais
		,CAST('0' AS int) AS IdDepartamento
        ,CAST('0' AS int) AS IdProvincia
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimDepartamentos#NA

	CREATE EXTERNAL TABLE ldw.DimDepartamentos (
	                DsDepartamento varchar(100),
					IdPais varchar(100),
					IdDepartamento int,
					IdProvincia int,
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimdepartamentos/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadDimDepartamentos
END
BEGIN --LINEAS
	CREATE EXTERNAL TABLE stg.lineas (
	letra varchar(5),
	nrodoc int,
	serie int,
	idempresa int,
	idLinea int,
	iddocumento varchar(10),
	cant int,
	codart int,
	bonif DECIMAL(10,2),
	precio DECIMAL(10,3),
	resto int,
	peso  DECIMAL(10,3),
	iva2  DECIMAL(12,5),
	iva1 DECIMAL(12,5),
	fechafac varchar(20),
	anulado bit,
	idComp varchar(5),
	seqstock int,
	iva1fis DECIMAL(10,2)
	)
	WITH (
	    LOCATION = 'chess/lineas.csv',
	    DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
    )
	exec ldw.UpdateFactLineas
END
BEGIN --LOCALIDADES
	CREATE EXTERNAL TABLE stg.localidades (
		idpais varchar(5),
		iddepartamento int,
		idprovincia int,
		idlocalidad int,
		dslocalidad varchar(100)
	)
	WITH (
		LOCATION = 'chess/localidades.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)
	CREATE EXTERNAL TABLE DimLocalidades#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimlocalidades/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('N/A' AS VARCHAR(5)) AS IdPais
		,CAST(0 AS INT) as IdDepartamento
		,CAST(0 AS INT) as IdProvincia
		,CAST(0 AS INT) as IdLocalidad
        ,CAST('N/A' AS VARCHAR(100)) AS DsLocalidad
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimLocalidades#NA

	CREATE EXTERNAL TABLE ldw.DimLocalidades (
	                IdPais varchar(5),
					IdDepartamento int,
					IdProvincia int,
					IdLocalidad int,
					DsLocalidad varchar(100),
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimlocalidades/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadLocalidades
END
BEGIN --MASCARA
    CREATE EXTERNAL TABLE stg.mascara (
	    fechafac varchar(20),
	    idcliente int,
	    c_perso int,
	    letra varchar(5),
	    nrodoc int,
	    serie int,
	    iddocumento varchar(10),
	    idempresa int,
	    bonif DECIMAL(10,2),
	    bruto DECIMAL(10,2),
	    fecpaga varchar(20),
	    tipoiva varchar(5),
	    nropla int,
	    iva1 DECIMAL(10,2),
	    iva2 DECIMAL(10,2),
	    numcam int,
	    netogra DECIMAL(10,2),
	    nograva DECIMAL(10,2),
	    anulado bit,
	    idDepo int,
	    idSucur int,
	    idfuerzaventas int,
	    idAgru int,
	    origen varchar(20),
		rutadis int,
		ctacte int,
		tipopla varchar(6)
    )
    WITH (
	    LOCATION = 'chess/mascara.csv',
	    DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
    )
	exec ldw.UpdateFactMascara
END
BEGIN --MASCSTOCK
    CREATE EXTERNAL TABLE stg.mascstock (
	    nromov int,
		tipomov varchar(10),
		idcliente int,
		fechamov varchar(20)
    )
    WITH (
	    LOCATION = 'chess/mascstock.csv',
	    DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
    )
	exec ldw.UpdateFactMascStock
END
BEGIN --MEMO
    CREATE EXTERNAL TABLE stg.memo (
	    letra varchar(5),
		nrodoc int,
		idLinea int,
		serie int,
		iddocumento varchar(10),
		idempresa int,
		anulado bit,
		codart int,
		precio DECIMAL(10,2),
		fechafac varchar(20),
		idcliente int

    )
    WITH (
	    LOCATION = 'chess/memo.csv',
	    DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
    )
	exec ldw.UpdateFactMemo
END
BEGIN --MOTIVOS
	CREATE EXTERNAL TABLE stg.motivos (
		letra varchar(5),
		motivo varchar(60),
		nrodoc int,
		serie int,
		idrechazo int,
		idempresa int,
		iddocumento varchar(10)
	)
	WITH (
		LOCATION = 'chess/fonpell/motivos.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)
END
BEGIN --PERSCOM
	CREATE EXTERNAL TABLE stg.perscom (
		c_perso int,
		d_perso varchar(100),
		cargo char(1),
		c_super int,
		idSucur int,
		anulado bit,
		idfuerzaventas int
	)
	WITH (
		LOCATION = 'chess/perscom.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)
	CREATE EXTERNAL TABLE DimPersCom#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimperscom/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('0' AS int) AS C_Perso
        ,CAST('N/A' AS VARCHAR(100)) AS D_Perso
		,CAST('G' AS VARCHAR(1)) AS Cargo
		,CAST('0' AS int) AS C_Super
		,CAST('0' AS int) AS IdSucur
		,CAST('0' AS bit) AS Anulado
		,CAST('0' AS int) AS IdFuerzaVentas
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimPersCom#NA

	CREATE EXTERNAL TABLE ldw.DimPersCom (
	                C_Perso int,
					D_Perso varchar(100),
					Cargo char(1),
					C_Super int,
					IdSucur int,
					Anulado bit,
					IdFuerzaVentas int,
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimperscom/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadPerscom
END
BEGIN --PROVINS
	CREATE EXTERNAL TABLE stg.provins (
		codprovi varchar(100),
		descprovi varchar(100),
		idpais varchar(5),
		idprovincia int
	)
	WITH (
		LOCATION = 'chess/provins.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)
	CREATE EXTERNAL TABLE DimProvins#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimprovins/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('N/A' AS VARCHAR(100)) AS CodProvi
		,CAST('N/A' AS VARCHAR(100)) AS DescProvi
		,CAST('N/A' AS VARCHAR(100)) AS IdPais
        ,CAST('0' AS int) AS IdProvincia
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimProvins#NA

	CREATE EXTERNAL TABLE ldw.DimProvins (
	                CodProvi varchar(100),
					DescProvi varchar(100),
					IdPais varchar(5),
					IdProvincia int,
					Ver int
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimprovins/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadDimProvins
END
BEGIN-- rlagrupacionesarticulos
    CREATE EXTERNAL TABLE stg.rlagrupacionesarticulos (
	    IdFormaAgrupar VARCHAR(100),
	    IdAgrupacion VARCHAR(100),
	    CodArt INT)
    WITH (
	    LOCATION = 'chess/rlagrupacionesarticulos.csv',
	    DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
    )

    CREATE EXTERNAL TABLE DimRLAgrupacionesArticulos#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimrlagrupacionesarticulos/IdFormaAgrupar=LINPRODU/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('LINPRODU' AS VARCHAR(100)) AS IdFormaAgrupar,
        CAST('N/A' AS VARCHAR(100)) AS IdAgrupacion,
        CAST(0 AS INT) as CodArt,
        CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimRLAgrupacionesArticulos#NA

    CREATE EXTERNAL TABLE ldw.DimRLAgrupacionesArticulos (
	            IdFormaAgrupar VARCHAR(100),
                IdAgrupacion VARCHAR(100),
	            CodArt INT,
                Ver INT) 
    WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimrlagrupacionesarticulos/IdFormaAgrupar=*/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	EXEC ldw.LoadDimRLAgrupacionesArticulos
END
BEGIN --RUTASDEVENTA
	CREATE EXTERNAL TABLE stg.rutasdeventa (
		idsucur int,
		idfuerzaventas int,
		idmodoatencion varchar(5),
		ruta int,
		fecrutadesde varchar(20),
		fecrutahasta varchar(20),
		c_perso int,
		anulado bit,
		diasvis varchar(100),
		d_ruta varchar(100)
	)
	WITH (
		LOCATION = 'chess/fonpell/rutasdeventa.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)

	CREATE EXTERNAL TABLE DimRutasDeVenta#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimrutasdeventa/IdSucur=0/Ruta=0/IdFuerzaVentas=0/IdModoAtencion=NA/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST(0 AS INT) AS IdSucur,
        CAST(0 AS INT) AS IdFuerzaVentas,
		CAST('NA' AS VARCHAR(5)) AS IdModoAtencion,
		CAST(0 AS INT) AS Ruta,
		CAST('N/A' AS VARCHAR(20)) AS FecRutaDesde,
		CAST('N/A' AS VARCHAR(20)) AS FecrutaHasta,
		CAST(0 AS INT) AS C_Perso,
		CAST(0 AS bit) AS Anulado,
		CAST('N/A' AS VARCHAR(100)) AS DiasVis,
		CAST('N/A' AS VARCHAR(100)) AS D_Ruta,
        CAST(0 AS INT) as Ver

	DROP EXTERNAL TABLE DimRutasDeVenta#NA

	CREATE EXTERNAL TABLE ldw.DimRutasDeVenta (
	    idsucur int,
		idfuerzaventas int,
		idmodoatencion varchar(5),
		ruta int,
		fecrutadesde varchar(20),
		fecrutahasta varchar(20),
		c_perso int,
		anulado bit,
		diasvis varchar(100),
		d_ruta varchar(100),
        Ver INT) 
    WITH (
		LOCATION = 'gold/chess/fonpell/dimension/dimrutasdeventa/IdSucur=*/Ruta=*/IdFuerzaVentas=*/IdModoAtencion=*/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )


END
BEGIN --TIPONEGO
	CREATE EXTERNAL TABLE stg.tiponego (
		codnego int,
		descnego varchar(100)
	)
	WITH (
		LOCATION = 'chess/tiponego.csv',
		DATA_SOURCE = eds_openstorage_delfosldw,  
        FILE_FORMAT = eff_csv_source
	)
	CREATE EXTERNAL TABLE DimTipoNego#NA 
    WITH (
        LOCATION = '/gold/chess/fonpell/dimension/dimtiponego/Ver=0',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )  
    AS
    SELECT 
        CAST('0' AS int) AS CodNego
        ,CAST('N/A' AS VARCHAR(100)) AS DescNego
        ,CAST(0 AS INT) as Ver

    DROP EXTERNAL TABLE DimTipoNego#NA

	CREATE EXTERNAL TABLE ldw.DimTipoNego (
	                CodNego INT,
	                DescNego varchar(100),
                    Ver INT
                ) WITH (
        LOCATION = 'gold/chess/fonpell/dimension/dimtiponego/Ver=*/*.parquet',
        DATA_SOURCE = eds_delfosldw,  
        FILE_FORMAT = eff_delta_delfos
    )
	exec ldw.LoadTipoNego
END

