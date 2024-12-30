--a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.
SELECT COUNT(cra.model) AS `cantidad_alquileres_autos`
FROM car_rental_analytics cra
WHERE cra.fueltype IN ('hybrid', 'electric')
AND cra.rating <= 4;

--b. los 5 estados con menor cantidad de alquileres (mostrar query y visualización)


--c. los 10 modelos (junto con su marca) de autos más rentados (mostrar query y visualización)


--d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015


--e. las 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico)


--f. el promedio de reviews, segmentando por tipo de combustible
