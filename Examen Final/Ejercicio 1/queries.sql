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

SELECT count(adt.denominacion) AS numero_aeropuertos
FROM aeropuerto_detalles_tabla adt;

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

--RANK() OVER (ORDER BY SUM(od.quantity * od.unit_price) DESC) AS "Rank"

SELECT 
	RANK() OVER (ORDER BY SUM(AT.pasajeros) DESC) AS Rank,
	AT.aerolinea_nombre,
	SUM(AT.pasajeros) AS cantidad_pasajeros
FROM aeropuerto_tabla AT
WHERE AT.fecha BETWEEN "2021-01-01" AND "2022-06-30"
AND (AT.aerolinea_nombre IS NOT NULL) AND (AT.aerolinea_nombre <> "0")
GROUP BY AT.aerolinea_nombre
LIMIT 10;