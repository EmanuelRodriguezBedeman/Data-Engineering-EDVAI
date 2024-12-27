<div align="center">
  <h1>Ejercicio 2</h1>
  <h3>Alquiler de automóviles</h3>
</div>

Una de las empresas líderes en alquileres de automóviles solicita una serie de dashboards y reportes para poder basar sus decisiones en datos. Entre los indicadores mencionados se encuentran total de alquileres, segmentación por tipo de combustible, lugar, marca y modelo de automóvil, valoración de cada alquiler, etc.

Como Data Engineer debe crear y automatizar el pipeline para tener como resultado los datos listos para ser visualizados y responder las preguntas de negocio.

**1.** Crear en `hive` una database `car_rental_db` y dentro una tabla llamada `car_rental_analytics`, con estos campos:

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

**2.** Crear script para el ingest de estos dos files

* https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv
* https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv

> **Sugerencia**: descargar el segundo archivo con un comando similar al abajo mencionado, ya que al tener caracteres como ‘&’ falla si no se le asignan comillas. Adicionalmente, el parámetro -O permite asignarle un nombre más legible al archivo descargado

```bash
wget -P ruta_destino -O ruta_destino/nombre_archivo.csv ruta_al_archivo
```

*Info del dataset*: https://www.kaggle.com/datasets/kushleshkumar/cornell-car-rental-dataset

```bash
nano car_rental_ingest.sh
```

```bash
#!/bin/bash

# Mensaje de inicio
echo "****** Inicio Ingesta Alquiler Automoviles ******"

# Directorio landing en hadoop
LANDING_DIR="/home/hadoop/landing"

# Directorio destino en HDFS
DEST_DIR="/ingest"

# Link al bucket
LINK="https://dataengineerpublic.blob.core.windows.net/data-engineer/"

# Nombre archivos
CAR_RENTAL="CarRentalData.csv"
GEOREF="georef-united-states-of-america-state.csv"

# Descarga archivos
wget -P $LANDING_DIR $LINK$CAR_RENTAL
wget -P $LANDING_DIR -O $LANDING_DI/$GEOREF $LINK$GEOREF

# Mover archivos a HDFS
hdfs dfs -put $LANDING_DIR/$CAR_RENTAL $DEST_DIR
hdfs dfs -put $LANDING_DIR/$GEOREF $DEST_DIR

# Remueve archivos
rm $LANDING_DIR/$CAR_RENTAL
rm $LANDING_DIR/$GEOREF

# Mensaje de finalizacion
echo "\n****** Fin Ingesta Alquiler Automoviles ******"
```

![captura comandos punto 2](image-1.png)

```bash
chmod 555 car_rental_ingest.sh
```

**3.** Crear un script para tomar el archivo desde HDFS y hacer las siguientes transformaciones:
* En donde sea necesario, modificar los nombres de las columnas. Evitar espacios y puntos (reemplazar por _ ). Evitar nombres de columna largos
* Redondear los float de ‘rating’ y castear a int.
* Joinear ambos files
* Eliminar los registros con rating nulo
* Cambiar mayúsculas por minúsculas en ‘fuelType’
* Excluir el estado Texas \
Finalmente insertar en Hive el resultado.

```python
# Import librerias y creacion de sesion en Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Car Rental") \
    .enableHiveSupport() \
    .getOrCreate()

# Carga de datos
car_rental = spark.read.option('header', 'true').option('sep', ';').csv('hdfs://172.17.0.2:9000/ingest/CarRentalData.csv')
georef = spark.read.option('header', 'true').option('sep', ';').csv('hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv')

# Columnas originales
rental_columns = car_rental.columns

# Columnas modificadas ('_' en lugar de '.')
new_columns = [col.replace('.', '_') if '.' in col else col for col in rental_columns]

# DF con columnas modificadas
car_rental_mod = car_rental.toDF(*new_columns)
```