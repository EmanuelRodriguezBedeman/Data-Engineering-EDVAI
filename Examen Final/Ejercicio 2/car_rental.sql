--a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.
SELECT COUNT(cra.model) AS `cantidad_alquileres_autos`
FROM car_rental_analytics cra
WHERE cra.fueltype IN ('hybrid', 'electric')
AND cra.rating <= 4;

--b. Los 5 estados con menor cantidad de alquileres (mostrar query y visualización)
SELECT
	ROW_NUMBER() OVER (ORDER BY COUNT(cra.state_name)) AS `Rank`,
	cra.state_name,
	COUNT(cra.state_name) AS `cantidad_alquileres`
FROM car_rental_analytics cra
GROUP BY cra.state_name
LIMIT 5;

--c. Los 10 modelos (junto con su marca) de autos más rentados (mostrar query y visualización)
SELECT
	ROW_NUMBER() OVER (ORDER BY COUNT(cra.model) DESC) AS `Rank`,
	cra.model AS `modelo`,
	cra.make `marca`,
	COUNT(cra.model) AS `cantidad_alquileres`
FROM car_rental_analytics cra
GROUP BY cra.model, cra.make
LIMIT 10;

--d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015


--e. Las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)


--f. El promedio de reviews, segmentando por tipo de combustible
