## Resolucion ejercitacion 9

1. Crear una base de datos en Hive llamada `northwind_analytics`

![creacion base de datos](imgs/image.png)

```sql
CREATE DATABASE northwind_analytics;
```

2. Crear un script para importar un archivo .parquet de la base northwind que contenga la lista de clientes junto a la cantidad de productos vendidos ordenados de mayor a menor (campos customer_id, company_name, productos_vendidos). Luego ingestar el archivo a HDFS (carpeta /sqoop/ingest/clientes). Pasar la password en un archivo.

```bash
/usr/lib/sqoop/bin/sqoop import \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres \
    --password-file file:///home/hadoop/scripts/sqoop.pass \
    --query "SELECT c.customer_id, c.company_name, od.quantity AS productos_vendidos FROM customers c INNER JOIN orders o ON o.customer_id = c.customer_id INNER JOIN order_details od ON od.order_id = o.order_id WHERE \$CONDITIONS ORDER BY od.quantity DESC" \
    --m 1 \
    --target-dir /sqoop/ingest/clientes \
    --as-parquetfile \
    --delete-target-dir
```

3. Crear un script para importar un archivo .parquet de la base northwind que contenga la lista de órdenes junto a qué empresa realizó cada pedido (campos order_id, shipped_date, company_name, phone). Luego ingestar el archivo a HDFS (carpeta /sqoop/ingest/envíos). Pasar la password en un archivo.

```bash
/usr/lib/sqoop/bin/sqoop import \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres\
    --password-file file:///home/hadoop/scripts/sqoop.pass \
    --query "SELECT o.order_id, o.shipped_date, c.company_name, c.phone FROM orders o INNER JOIN customers c ON c.customer_id = o.customer_id AND \$CONDITIONS" \
    --m 1 \
    --target-dir /sqoop/ingest/envios \
    --as-parquetfile \
    --delete-target-dir
```

4. Crear un script para importar un archivo .parquet de la base northwind que contenga la lista de detalles de órdenes (campos order_id, unit_price, quantity, discount). Luego ingestar el archivo a HDFS (carpeta /sqoop/ingest/order_details). Pasar la password en un archivo

```bash
/usr/lib/sqoop/bin/sqoop import \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres\
    --password-file file:///home/hadoop/scripts/sqoop.pass \
    --query "SELECT od.order_id, od.unit_price, od.quantity, od.discount FROM order_details od WHERE \$CONDITIONS" \
    --m 1 \
    --target-dir /sqoop/ingest/order_details \
    --as-parquetfile \
    --delete-target-dir
```

5. Generar un archivo .py que permita mediante `Spark` insertar en `Hive` en la db `northwind_analytics` en la tabla `products_sold`, los datos del punto 2, pero solamente aquellas compañías en las que la cantidad de productos vendidos fue mayor al promedio.

Creacion tabla `products_sold` en `Hive`:

```sql
CREATE EXTERNAL TABLE products_sold(
    customer_id STRING,
    company_name STRING,
    productos_vendidos INTEGER
    )
COMMENT "Tabla products_sold ejercicio 9, punto 5"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/products_sold';
```

Transformacion `products_sold` en `PySpark`:

```python
# Imports & Creates Spark's session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Northwind products_sold") \
    .enableHiveSupport() \
    .getOrCreate()

# Data Load
clientes = spark.read.option('header', 'true').parquet('hdfs://172.17.0.2:9000/sqoop/ingest/clientes/')

# Obtains the average of products sold
promedio_ventas = clientes.agg({'productos_vendidos': 'avg'}).first()[0]

# Filters companies which sold above the average
clientes_final = clientes.filter(col('productos_vendidos') > promedio_ventas)

# Inserts data into Hive (DB nortwind_analytics, table: products_sold)
clientes_final.write.insertInto('northwind_analytics.products_sold')
```

6. Generar un archivo .py que permita mediante `Spark` insertar en `Hive` en la tabla `products_sent`, los datos del punto 3 y 4, de manera tal que se vean las columnas (`order_id, shipped_date, company_name, phone, unit_price_discount (unit_price with discount), quantity, total_price unit_price_discount * quantity`). Solo de aquellos pedidos que hayan tenido descuento.

Creacion tabla `products_sent` en `Hive`:

```sql
CREATE EXTERNAL TABLE products_sent(
    order_id INT,
    shipped_date BIGINT,
    company_name STRING,
    phone STRING,
    unit_price_discount FLOAT,
    quantity INT,
    total_price FLOAT
)
COMMENT "Tabla products_sent"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/products_sent';
```

Transformacion `envios` y `order_details` para tabla `products_sent` en `PySpark`:

```python
# Import libaries & creates Spark's session
from pyspark.sql import SparkSession
from pyspark.sql.functions import round, col

spark = SparkSession.builder \
    .appName("Northwind products_sent") \
    .enableHiveSupport() \
    .getOrCreate()

# Data Load
envios = spark.read.option('header', 'true').parquet('hdfs://172.17.0.2:9000/sqoop/ingest/envios/')
orders = spark.read.option('header', 'true').parquet('hdfs://172.17.0.2:9000/sqoop/ingest/order_details/')

# Filters discount > 0
orders_filtered = orders.filter(col('discount') > 0)

# Discount value
orders_price_discount = orders_filtered.withColumn('value_discount', round(col('unit_price') * col('discount'), 1))

# Adds variable unit_price_discount
orders_discounted = orders_price_discount.withColumn('unit_price_discount', col('unit_price') - col('value_discount'))

# Adds variable total_price
order_total = orders_discounted.withColumn('total_price', col('unit_price_discount') * col('quantity'))

# Filters orders by useful variables for union
orders_final = order_total.drop('unit_price', 'discount', 'value_discount')

# Joins dataframes
df_joined = envios.join(orders_final, on='order_id')

# Order columns

## Gets the columns
columns = df_joined.columns

## Changes columns places by index
columns[4], columns[5] = columns[5], columns[4]

## Changes columns order by the requested order
df_final = df_joined.select(columns)

# Inserts data into Hive (BD: northwind_analytics, table: products_sent)
df_final.write.insertInto('northwind_analytics.products_sent')
```

7. Realizar un proceso automático en `Airflow` que orqueste los pipelines creados en los puntos anteriores. Crear un grupo para la etapa de ingest y otro para la etapa de process. Correrlo y mostrar una captura de pantalla (del DAG y del resultado en la base de datos)

```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='Northwind',
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

    with TaskGroup("Ingest", tooltip="Task group for the ingest process") as ingest:

        ingest_clientes = BashOperator(
            task_id="clientes",
            bash_command= '/usr/bin/sh /home/hadoop/scripts/ingest_clientes.sh '
        )

        ingest_envios = BashOperator(
            task_id="envios",
            bash_command= '/usr/bin/sh /home/hadoop/scripts/ingest_envios.sh '
        )

        ingest_orders = BashOperator(
            task_id="orders",
            bash_command= '/usr/bin/sh /home/hadoop/scripts/ingest_orders.sh '
        )

    with TaskGroup("Transform", tooltip="Task group for the transform process") as transform:

        products_sold = BashOperator(
            task_id="products_sold",
            bash_command= 'ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/products_sold_transformation.py '
        )

        products_sent = BashOperator(
            task_id="products_sent",
            bash_command= 'ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/products_sent_transformation.py '
        )

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    inicia_proceso >> ingest >> transform >>finaliza_proceso

if __name__ == "__main__":
    dag.cli()
```

![overhead dag](imgs/image-1.png)

![full dag](imgs/image-2.png)