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
-- Vuelos de salida con su ciudad
CREATE VIEW vuelos_salida AS
SELECT 
  at.fecha,
  at.hourautc,
  at.aeropuerto_salida AS `codigo_aeropuerto_salida`,
  adt.REF AS `ciudad_de_salida`,
  at.pasajeros
FROM (
  SELECT 
    AT.fecha,
    AT.hourautc,
    AT.aeropuerto AS `aeropuerto_salida`,
    AT.pasajeros
  FROM aeropuerto_tabla AT
  WHERE AT.fecha BETWEEN "2022-01-01" AND "2022-06-30"
  AND AT.tipo_de_movimiento = "Despegue"
  UNION
  SELECT 
    AT.fecha,
    AT.hourautc,
    AT.origen_destino AS `aeropuerto_salida`,
    AT.pasajeros
  FROM aeropuerto_tabla AT
  WHERE AT.fecha BETWEEN "2022-01-01" AND "2022-06-30"
  AND AT.tipo_de_movimiento = "Aterrizaje"
) at
INNER JOIN aeropuerto_detalles_tabla adt
ON adt.aeropuerto = at.aeropuerto_salida;

-- Vuelos de arribo con su ciudad
CREATE VIEW vuelos_arribo AS
SELECT 
  at.fecha,
  at.hourautc,
  at.aeropuerto_arribo AS `codigo_aeropuerto_arribo`,
  adt.REF AS `ciudad_de_arribo`,
  at.pasajeros
FROM (
  SELECT 
    AT.fecha,
    AT.hourautc,
    AT.aeropuerto AS `aeropuerto_arribo`,
    AT.pasajeros
  FROM aeropuerto_tabla AT
  WHERE AT.fecha BETWEEN "2022-01-01" AND "2022-06-30"
  AND AT.tipo_de_movimiento = "Aterrizaje"
  UNION
  SELECT 
    AT.fecha,
    AT.hourautc,
    AT.origen_destino AS `aeropuerto_arribo`,
    AT.pasajeros
  FROM aeropuerto_tabla AT
  WHERE AT.fecha BETWEEN "2022-01-01" AND "2022-06-30"
  AND AT.tipo_de_movimiento = "Despegue"
) at
INNER JOIN aeropuerto_detalles_tabla adt
ON adt.aeropuerto = at.aeropuerto_arribo;

SELECT
	vs.fecha,
	vs.hourautc,
	vs.codigo_aeropuerto_salida,
	vs.ciudad_de_salida,
	va.codigo_aeropuerto_arribo,
	va.ciudad_de_arribo,
	vs.pasajeros + va.pasajeros AS `pasajeros`
FROM vuelos_salida vs
INNER JOIN vuelos_arribo va
ON vs.fecha = va.fecha
AND vs.hourautc = va.hourautc
ORDER BY fecha DESC;

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
SELECT 
    aeronave,
    COUNT(*) AS cantidad_de_vuelos
FROM  aeropuerto_tabla
WHERE 
    fecha BETWEEN '2021-01-01' AND '2022-06-30'
    AND aeropuerto IN (
        SELECT 
            aeropuerto
        FROM 
            aeropuerto_detalles_tabla
        WHERE 
            provincia IN ('CIUDAD AUTÓNOMA DE BUENOS AIRES', 'BUENOS AIRES')
    )
    AND (aeronave IS NOT NULL) 
    AND (aeronave <> "0")
GROUP BY aeronave
ORDER BY cantidad_de_vuelos DESC
LIMIT 10;