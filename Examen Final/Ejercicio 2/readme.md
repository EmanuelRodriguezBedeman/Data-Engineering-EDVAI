<div align="center">
  <h1>Ejercicio 2</h1>
  <h3>Alquiler de automóviles</h3>
</div>

Una de las empresas líderes en alquileres de automóviles solicita una serie de dashboards y reportes para poder basar sus decisiones en datos. Entre los indicadores mencionados se encuentran total de alquileres, segmentación por tipo de combustible, lugar, marca y modelo de automóvil, valoración de cada alquiler, etc.

Como Data Engineer debe crear y automatizar el pipeline para tener como resultado los datos listos para ser visualizados y responder las preguntas de negocio.

1. Crear en `hive` una database `car_rental_db` y dentro una tabla llamada `car_rental_analytics`, con estos campos:

<div align="center">
    <table>
        <tr>
            <th>Campo</th>
            <th>Tipo</th>
        </tr>
        <tr>
            <td>fuelType</td>
            <td>string</td>
        </tr>
        <tr>
            <td>rating</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>renterTripsTaken</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>reviewCount</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>city</td>
            <td>string</td>
        </tr>
        <tr>
            <td>state_name</td>
            <td>string</td>
        </tr>
        <tr>
            <td>owner_id</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>rate_daily</td>
            <td>integer</td>
        </tr>
        <tr>
            <td>make</td>
            <td>string</td>
        </tr>
        <tr>
            <td>model</td>
            <td>string</td>
        </tr>
        <tr>
            <td>year</td>
            <td>integer</td>
        </tr>
    </table>
</div>

```SQL
CREATE DATABASE car_rental_db;
```

```bash
CREATE EXTERNAL TABLE car_rental_analytics(
    fuelType STRING,
    rating INTEGER,
    renterTripsTaken INTEGER,
    reviewCount INTEGER,
    city STRING,
    state_name STRING,
    owner_id INTEGER,
    rate_daily INTEGER,
    make STRING,
    model STRING,
    year INTEGER
    )
COMMENT "Tabla car_rental_analytics para examen final ejercicio 2"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/car_rental_analytics';
```

![screenshot comandos en consola creacion bd y tabla](image.png)

