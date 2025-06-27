CREATE OR ALTER  PROCEDURE [helpers].[spGetDatesToUpdate]
@Table VARCHAR(20) = 'VentasResumen'
AS
BEGIN
	IF @Table = 'Memo' 
	BEGIN
		SELECT DISTINCT CONVERT(date, m.fechaComprobate,103) AS FechaFac
		FROM 
			bronze.VentasResumen m
		ORDER BY 1
	END
	ELSE IF @Table = 'MascStock' 
	BEGIN
		SELECT DISTINCT CONVERT(date, m.fechaComprobate,103) AS FechaFac
		FROM 
			bronze.VentasResumen m
		ORDER BY 1
	END
	ELSE IF @Table = 'Lineas' 
	BEGIN
		SELECT DISTINCT CONVERT(date, m.fechaComprobate,103) AS FechaFac
		FROM 
			bronze.VentasResumen m
		ORDER BY 1
	END
	ELSE
	BEGIN
		SELECT DISTINCT CONVERT(date, m.fechaComprobate,103) AS FechaFac
		FROM 
			bronze.VentasResumen m
		ORDER BY 1
	END
END