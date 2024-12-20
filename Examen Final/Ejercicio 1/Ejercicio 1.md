<div align="center">
  <h1>Ejercicio 1</h1>
  <h3>Aviacion Civil</h3>
</div>

*La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino, como puede ser: cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades de partidas y aterrizajes entre fechas determinadas, etc.*

*Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y hacer sus recomendaciones con respecto al estado actual.*

Listado de vuelos realizados:
https://datos.gob.ar/lv/dataset/transporte-aterrizajes-despegues-procesados-por-administracionnacional-aviacion-civil-anac


Listado de detalles de aeropuertos de Argentina:
https://datos.transporte.gob.ar/dataset/lista-aeropuertos

<h1>TAREAS</h1>

1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina:

2021:
https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv

2022:
https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv

Aeropuertos_detalles:
https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv

**Archivo** `aviacion_ingest.sh`:

```bash
#!/bin/bash

# Mensaje de inicio
echo "****** Inicio Ingesta Aviacion Civil ******"

# Variable directorio landing
LANDING_DIR="/home/hadoop/landing"

# Array con nombre de archivos
ARCHIVOS=("2021-informe-ministerio.csv" "202206-informe-ministerio.csv" "aeropuertos_detalle.csv")

# Loop para realizar ingesta
for ARCHIVO in "${ARCHIVOS[@]}"; do
    wget -P $LANDING_DIR https://dataengineerpublic.blob.core.windows.net/data-engineer/$ARCHIVO

    hdfs dfs -put $LANDING_DIR/$ARCHIVO /ingest

    rm $LANDING_DIR/$ARCHIVO
done

# Mensaje de fin
echo "\n****** Fin Ingesta Aviacion Civil ******"
```

2. Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022 (2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle de los aeropuertos (aeropuertos_detalle.csv).

Crea base de datos `aviacion`:

```bash
CREATE DATABASE aviacion;
```

Crea tabla `aeropuerto_tabla`:

```bash
CREATE EXTERNAL TABLE aeropuerto_tabla(
    fecha DATE,
    houraUTC string,
    clase_de_vuelo STRING,
    clasificacion_de_vuelo STRING,
    tipo_de_movimiento STRING,
    aeropuerto STRING,
    origen_destino STRING,
    aerolinea_nombre STRING,
    aeronave STRING,
    pasajeros INT
    )
COMMENT "Tabla aeropuerto_tabla para examen final ejercicio 1"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/aeropuerto_tabla';
```

Crea tabla `aeropuerto_detalles_tabla`:

```bash
CREATE EXTERNAL TABLE aeropuerto_detalles_tabla(
    aeropuerto STRING,
    oac STRING,
    iata STRING,
    tipo STRING,
    denominacion STRING,
    coordenadas STRING,
    latitud STRING,
    longitud STRING,
    elev FLOAT,
    uom_elev STRING,
    ref STRING,
    distancia_ref float,
    direccion_ref STRING,
    condicion STRING,
    control STRING,
    region STRING,
    uso STRING,
    trafico STRING,
    sna STRING,
    concesionado STRING,
    provincia STRING
    )
COMMENT "Tabla aeropuerto_detalles_tabla para examen final ejercicio 1"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/aeropuerto_detalles_tabla';
```

3. Realizar un proceso automático orquestado por airflow que ingeste los archivos previamente mencionados entre las fechas *01/01/2021* y *30/06/2022* en las dos columnas creadas.

Los archivos `202206-informe-ministerio.csv` y `202206-informe-ministerio.csv` → en la tabla `aeropuerto_tabla`.

El archivo aeropuertos_detalle.csv → en la tabla `aeropuerto_detalles_tabla`.

Archivo `aviacion_dag.py`:

```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='Aviacion',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:
    
    inicia_proceso = DummyOperator(
        task_id='inicia_proceso',
    )

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    ingest = BashOperator(
        task_id='ingesta',
        bash_command='/usr/bin/sh /home/hadoop/scripts/aviacion_ingest.sh ',
    )

    transform = BashOperator(
        task_id='transformacion',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/aviacion_transformacion.py ',
    )

    inicia_proceso >> ingest >> transform >>finaliza_proceso

if __name__ == "__main__":
    dag.cli()
```

4. Realizar las siguiente transformaciones en los pipelines de datos:
* Eliminar la columna inhab ya que no se utilizará para el análisis
* Eliminar la columna fir ya que no se utilizará para el análisis
* Eliminar la columna “calidad del dato” ya que no se utilizará para el análisis
* Filtrar los vuelos internacionales ya que solamente se analizarán los vuelos domésticos
* En el campo pasajeros si se encuentran campos en Null convertirlos en 0 (cero)
* En el campo distancia_ref si se encuentran campos en Null convertirlos en 0 (cero)

```python
# Import librerias y creacion de sesion en Spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Aviacion") \
    .enableHiveSupport() \
    .getOrCreate()

# Carga de datos
vuelos_2021 = spark.read.option('header', 'true').option('sep', ';').csv('hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv')
vuelos_2022 = spark.read.option('header', 'true').option('sep', ';').csv('hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv')
aeropuertos = spark.read.option('header', 'true').option('sep', ';').csv('hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv')

# Union vuelos 2021 y 2022
vuelos = vuelos_2021.union(vuelos_2022)

# Tabla vuelos
# Casting de las columnas "Fecha" y "Pasajeros"
vuelos_mod = vuelos \
    .withColumn("Pasajeros", F.col("Pasajeros").cast("int")) \
    .withColumn("Fecha", F.to_date(vuelos["Fecha"], "dd/MM/yyyy").alias("Fecha"))

# Tabla vuelos
# Filtra por vuelos domesticos, elimina columna 'calidad del dato' y reemplaza nulos de la columna 'pasajeros' por ceros
vuelos_mod_domesticos_filtered = vuelos_mod \
    .filter(vuelos_mod['Clasificación Vuelo'] == "Domestico") \
    .drop('Calidad dato') \
    .fillna(0, 'pasajeros')

# Tabla aeropuertos_detalle
# Casting de variables elev y distancia_ref
aeropuertos_mod = aeropuertos \
    .withColumn("elev", aeropuertos["elev"].cast("float")) \
    .withColumn("distancia_ref", aeropuertos["distancia_ref"].cast("float"))

# Tabla aeropuertos_detalle
# Elimina columnas 'inhab', 'fir' y reemplaza nulos de la columna 'distancia_ref' por ceros. 
aeropuertos_transformado = aeropuertos_mod \
    .fillna(0, "distancia_ref") \
    .drop('inhab', 'fir')

# Inserta tabla "vuelos_mod_domesticos_filtered" en la BD aviacion, en la tabla 'aeropuerto_tabla'
vuelos_mod_domesticos_filtered.write.insertInto("aviacion.aeropuerto_tabla")

# Inserta tabla "detalle_aeropuertos" en la BD aviacion, en la tabla 'aeropuerto_detalles_tabla'
aeropuertos_transformado.write.insertInto("aviacion.aeropuerto_detalles_tabla")
```