CREATE OR ALTER VIEW [gold].[Agrupacion]
AS
SELECT 
	[idFormaAgrupar]
    ,[desFormaAgrupar]
    ,[idArticulo]
    ,[idAgrupacion]
    ,[desAgrupacion]
FROM 
[silver].[EAgrupacione] a
WHERE 
    a.Ver <> 0
AND a.Ver = (
        SELECT MAX(a2.Ver)
        FROM
            silver.EAgrupacione a2
        WHERE
				a.idArticulo = a2.idArticulo
			AND a.idFormaAgrupar = a2.idFormaAgrupar
			AND a.idAgrupacion = a2.idAgrupacion
    )
go
CREATE OR ALTER VIEW [gold].[Articulo_Agrupacion]
AS
SELECT 
	Q1.idArticulo
	,a1.idAgrupacion AS FAMSM
	,a1.desAgrupacion AS FAMSM_DESC
	,a2.idAgrupacion AS FAMSUBSM
	,a2.desAgrupacion AS FAMSUBSM_DESC
	,a3.idAgrupacion AS FAMILIA
	,a3.desAgrupacion AS FAMILIA_DESC
	,a4.idAgrupacion AS MARCA
	,a4.desAgrupacion AS MARCA_DESC
	,a5.idAgrupacion AS PROVEED	
	,a5.desAgrupacion AS PROVEED_DESC
	,a6.idAgrupacion AS FABRICAN
	,a6.desAgrupacion AS FABRICAN_DESC
FROM
	(
		SELECT DISTINCT
			idArticulo
		FROM gold.Agrupacion
		WHERE
			idFormaAgrupar in ('FAMSM','FAMSUBSM','FAMILIA','MARCA','PROVEED','FABRICAN')
	) Q1
LEFT JOIN gold.Agrupacion a1 ON a1.idArticulo = Q1.idArticulo AND a1.idFormaAgrupar = 'FAMSM'
LEFT JOIN gold.Agrupacion a2 ON a2.idArticulo = Q1.idArticulo AND a2.idFormaAgrupar = 'FAMSUBSM'
LEFT JOIN gold.Agrupacion a3 ON a3.idArticulo = Q1.idArticulo AND a3.idFormaAgrupar = 'FAMILIA'
LEFT JOIN gold.Agrupacion a4 ON a4.idArticulo = Q1.idArticulo AND a4.idFormaAgrupar = 'MARCA'
LEFT JOIN gold.Agrupacion a5 ON a5.idArticulo = Q1.idArticulo AND a5.idFormaAgrupar = 'PROVEED'
LEFT JOIN gold.Agrupacion a6 ON a6.idArticulo = Q1.idArticulo AND a6.idFormaAgrupar = 'FABRICAN'