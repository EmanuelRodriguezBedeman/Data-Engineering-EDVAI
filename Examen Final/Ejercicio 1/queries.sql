-- Determinar la cantidad de vuelos entre las fechas 01/12/2021 y 31/01/2022. Mostrar consulta y Resultado de la query
SELECT count(*) AS cantidad_vuelos
FROM aeropuerto_tabla at
WHERE at.fecha BETWEEN "2021-12-01" AND "2022-01-31";

SELECT AT.fecha
FROM aeropuerto_tabla AT
LIMIT 10;