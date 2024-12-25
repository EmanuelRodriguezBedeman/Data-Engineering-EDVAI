-- 6. Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022.
SELECT count(AT.fecha) AS cantidad_vuelos
FROM aeropuerto_tabla AT
WHERE AT.fecha BETWEEN "2021-12-01" AND "2022-01-31";

-- 7. Cantidad de pasajeros que viajaron en Aerolíneas Argentinas entre el 01/01/2021 y 30/06/2022.
SELECT SUM(AT.pasajeros) AS cantidad_pasajeros
FROM aeropuerto_tabla AT
WHERE 
	AT.aerolinea_nombre = "AEROLINEAS ARGENTINAS SA" AND
	AT.fecha BETWEEN "2021-01-01" AND "2022-06-30";

-- 8. Mostrar fecha, hora, código aeropuerto salida, ciudad de salida, código de aeropuerto de arribo, ciudad de arribo, y cantidad de pasajeros de cada vuelo, entre el 01/01/2022 y el 30/06/2022 ordenados por fecha de manera descendiente.
SELECT 
	AT.fecha,
	AT.hourautc,
	AT.pasajeros
FROM aeropuerto_tabla AT
WHERE AT.fecha BETWEEN "2022-01-01" AND "2022-06-30"
ORDER BY AT.fecha DESC;

SELECT 
	AT.fecha,
	AT.hourautc,
	AT.pasajeros,
  CASE 
    WHEN `tipo_de_movimiento` = 'Aterrizaje' THEN 'Origen'
    WHEN `tipo_de_movimiento` = 'Despegue' THEN 'Destino'
  END `tipo_de_vuelo`
FROM aeropuerto_tabla AT
WHERE AT.fecha BETWEEN "2022-01-01" AND "2022-06-30"
ORDER BY AT.fecha DESC;

-- 9. Cuales son las 10 aerolíneas que más pasajeros llevaron entre el 01/01/2021 y el 30/06/2022 exceptuando aquellas aerolíneas que no tengan nombre.
SELECT 
	RANK() OVER (ORDER BY SUM(AT.pasajeros) DESC) AS Rank,
	AT.aerolinea_nombre,
	SUM(AT.pasajeros) AS cantidad_pasajeros
FROM aeropuerto_tabla AT
WHERE AT.fecha BETWEEN "2021-01-01" AND "2022-06-30"
AND (AT.aerolinea_nombre IS NOT NULL) AND (AT.aerolinea_nombre <> "0")
GROUP BY AT.aerolinea_nombre
LIMIT 10;

-- 10. Cuales son las 10 aeronaves más utilizadas entre el 01/01/2021 y el 30/06/22 que despegaron desde la Ciudad autónoma de Buenos Aires o de Buenos Aires, exceptuando aquellas aeronaves que no cuentan con nombre.
-- Vista temporal de aeropuertos en BsAs / CABA
CREATE VIEW aeropuertos_bsas AS
SELECT 
	adt.aeropuerto,
	adt.provincia
FROM aeropuerto_detalles_tabla adt
WHERE adt.provincia IN ("BUENOS AIRES", "CIUDAD AUTÓNOMA DE BUENOS AIRES");

-- Vista temporal de aeronaves en fecha pedida, no nulas y que despegan
CREATE VIEW aeronaves AS
SELECT
	AT.aeropuerto,
	AT.aeronave
FROM aeropuerto_tabla AT
WHERE AT.fecha BETWEEN "2021-01-01" AND "2022-06-30"
AND (AT.aeronave IS NOT NULL) AND (AT.aeronave <> "0")
AND AT.tipo_de_movimiento == "Despegue";

-- Top 10 aeronaves mas utilizadas, union vistas temporales
SELECT
	RANK() OVER (ORDER BY COUNT(a.aeronave) DESC) AS `Ranking`,
	a.aeronave AS `Aeronave`,
	COUNT(a.aeronave) AS `Numero aeronaves`
FROM aeronaves a
INNER JOIN aeropuertos_bsas bs
ON a.aeropuerto = bs.aeropuerto
WHERE bs.provincia = "CIUDAD AUTÓNOMA DE BUENOS AIRES"
GROUP BY a.aeronave
LIMIT 10;