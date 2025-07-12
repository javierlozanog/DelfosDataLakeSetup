:setvar pathInsert "D:\Projects 2024\DelfosV2\DelfosDataLakeSetup\Tables\silver\Insert"
:setvar pathUpdate "D:\Projects 2024\DelfosV2\DelfosDataLakeSetup\Tables\silver\Update"

:r $(pathInsert)\Agrupaciones.sql
go
:r $(pathInsert)\Articulo.sql
go
:r $(pathInsert)\Cliente.sql
go
:r $(pathUpdate)\Agrupaciones.sql
go
:r $(pathUpdate)\Articulo.sql
go
:r $(pathUpdate)\Cliente.sql