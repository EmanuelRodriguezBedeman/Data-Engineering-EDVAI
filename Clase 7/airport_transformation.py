# Import librerias y creacion de sesion en Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, year

spark = SparkSession.builder \
    .appName("Airport Trips") \
    .enableHiveSupport() \
    .getOrCreate()

# Carga de datos
df = spark.read.option('header', 'true').parquet(
    'hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet', 
    'hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet'
)

# Filtro por columnas, fecha entre enero y febrero del 2021, el tipo de pago = efectivo y origen o destino = aeropuerto
columnas = ["tpep_pickup_datetime", "airport_fee", "payment_type", "tolls_amount", "total_amount"]

df_airport = df.select(*columnas).filter(
    (month(df["tpep_pickup_datetime"]).isin([1, 2])) & 
    (year(df["tpep_pickup_datetime"]) == 2021) & 
    (df["payment_type"] == 2 ) &
    (df["airport_fee"] > 0)
)

# Inserta los datos en Hive
df_airport.write.insertInto("tripdata.airport_trips", overwrite=False)