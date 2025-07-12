:setvar path "D:\Projects 2024\DelfosV2\DelfosDataLakeSetup\Tables\silver\Initialize"
:r $(path)\Agrupaciones.sql
go
:r $(path)\Articulo.sql
go
:r $(path)\Cliente.sql
go
--:r $(path)\VentasResumen.sql
--go

:setvar path "D:\Projects 2024\DelfosV2\DelfosDataLakeSetup\Tables\gold"
:r $(path)\Agrupaciones.sql
go
:r $(path)\Articulo.sql
go
:r $(path)\Cliente.sql
go
--:r $(path)\VentasResumen.sql
--go
