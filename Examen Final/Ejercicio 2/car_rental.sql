--a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.
SELECT SUM(cra.rentertripstaken) AS cantidad_alquileres
FROM car_rental_analytics cra
WHERE cra.fueltype IN ('hybrid', 'electric')
AND cra.rating >= 4;

--b. Los 5 estados con menor cantidad de alquileres (mostrar query y visualización)
SELECT
	cra.state_name,
	SUM(cra.rentertripstaken) AS cantidad_alquileres
FROM car_rental_analytics cra
GROUP BY cra.state_name
ORDER BY cantidad_alquileres
LIMIT 5;

--c. Los 10 modelos (junto con su marca) de autos más rentados (mostrar query y visualización)
SELECT
	CONCAT(cra.make, ' ', cra.model) AS auto,
	SUM(cra.rentertripstaken) AS `cantidad_alquileres`
FROM car_rental_analytics cra
GROUP BY cra.model, cra.make
ORDER BY cantidad_alquileres DESC
LIMIT 10;

--d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015
SELECT
	cra.YEAR AS `anio_fabricacion`,
	SUM(cra.rentertripstaken) AS `cantidad_alquileres`
FROM car_rental_analytics cra
WHERE cra.year BETWEEN 2010 AND 2015
GROUP BY cra.YEAR;

--e. Las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)
--- FIXEAR RESULTADO Y VISUALIZACION
SELECT 
	cra.city AS `ciudad`,
	SUM(cra.rentertripstaken) AS `cantidad_alquileres`
FROM car_rental_analytics cra
WHERE cra.fueltype IN ('hybrid', 'electric')
GROUP BY cra.city
ORDER BY cantidad_alquileres DESC
LIMIT 5;

--f. El promedio de reviews, segmentando por tipo de combustible
SELECT
	AVG(cra.reviewcount) AS `promedio_reviews`,
	cra.fueltype AS `tipo_combustible`
FROM car_rental_analytics cra
WHERE cra.fueltype IS NOT NULL
GROUP BY cra.fueltype;