--ADEMAS HAY QUE BORRAR MANUALMENTE LOS ARCHIVOS .parquet en azure
DROP VIEW IF EXISTS [gold].[Agrupacion]
DROP VIEW IF EXISTS[gold].[Articulo]
DROP VIEW IF EXISTS[gold].[Articulo_Agrupacion]
DROP VIEW IF EXISTS[gold].[Cliente]
DROP VIEW IF EXISTS[gold].[VentasResumen]

DROP EXTERNAL TABLE [silver].[EAgrupacione]
DROP EXTERNAL TABLE [silver].[EArticulo]
DROP EXTERNAL TABLE [silver].[Cliente]
DROP EXTERNAL TABLE [silver].[VentasResumen]