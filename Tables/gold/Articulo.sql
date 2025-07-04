CREATE OR ALTER VIEW [gold].[Articulo]
AS
SELECT 
	[idArticulo]
    ,[desArticulo]
    ,[descDetallada]
    ,[unidadesBulto]
    ,[anulado]
    ,[fechaAlta]
    ,[factorVenta]
    ,[minimoVenta]
    ,[pesable]
    ,[pesoCotaSuperior]
    ,[pesoCotaInferior]
    ,[esCombo]
    ,[detalleComboImp]
    ,[detalleComboInf]
    ,[exentoIva]
    ,[inafecto]
    ,[exonerado]
    ,[ivaDiferencial]
    ,[tasaIva]
    ,[tasaInternos]
    ,[internosBulto]
    ,[tasaIibb]
    ,[esAlcoholico]
    ,[visibleMobile]
    ,[esComodatable]
    ,[desCortaArticulo]
    ,[idPresentacionBulto]
    ,[desPresentacionBulto]
    ,[idPresentacionUnidad]
    ,[desPresentacionUnidad]
    ,[idUnidadMedida]
    ,[desUnidadMedida]
    ,[valorUnidadMedida]
    ,[idArticuloEstadistico]
    ,[codBarraBulto]
    ,[codBarraUnidad]
    ,[tieneRetornables]
    ,[bultosPallet]
    ,[pisosPallet]
    ,[apilabilidad]
    ,[pesoBulto]
    ,[llevaFrescura]
    ,[diasBloqueo]
    ,[politicaStock]
    ,[diasVentana]
    ,[esActivoFijo]
    ,[cantidadPuertas]
    ,[unidadesFrente]
    ,[litrosRepago]
    ,[idArtUsado]
    ,[aniosAmortizacion]
FROM 
	[silver].[EArticulo] a
WHERE 
    a.Ver <> 0
AND a.Ver = (
        SELECT MAX(a2.Ver)
        FROM
            silver.EArticulo a2
        WHERE
            a.idArticulo = a2.idArticulo
    )