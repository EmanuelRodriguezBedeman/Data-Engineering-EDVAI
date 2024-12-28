# Import librerias y creacion de sesion en Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, round

spark = SparkSession.builder \
    .appName("Car Rental") \
    .enableHiveSupport() \
    .getOrCreate()

# Carga de datos
car = spark.read.option('header', 'true').option('sep', ',').csv('hdfs://172.17.0.2:9000/ingest/CarRentalData.csv')
georef = spark.read.option('header', 'true').option('sep', ';').csv('hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv')

# Columnas originales
rental_columns = car.columns

# Reemplaza puntos por guion bajo
new_columns = [col.replace('.', '_') if '.' in col else col for col in rental_columns]
car_rental = car.toDF(*new_columns)

# Columnas no utiles para el analisis
rm_car_cols = [
    'location_country', 
    'location_latitude', 
    'location_longitude', 
    'vehicle_type'
]

# Remueve columnas no utilizadas para el analisis
car_rental_mod = car_rental.drop(*rm_car_cols)

# Transform columna 'rating':
# - Redondeo y cast a int
# - Remueve valores nulos
car_rental_mod_rounded = car_rental_mod.withColumn('rating', round(car_rental_mod["rating"].cast("int"))) \
                            .dropna(subset=["rating"])

# ---- Transformacion tabla georef ----
# Filtro para obtener solo estados de la tabla georef
georef_filtered = georef.filter('type = "state"')

# Obtengo solamente las columnas utilizadas para el analisis de la tabla georef
georef_mod = georef_filtered.select(
    'United States Postal Service state abbreviation', 
    'Official Name State'
)

# Renombrado de columnas de la tabla georef
georef_mod = georef_mod.withColumnRenamed('United States Postal Service state abbreviation', 'location_state')
georef_mod = georef_mod.withColumnRenamed('Official Name State', 'state_name')

# Join de ambos files
df_joined = car_rental_mod_rounded.join(georef_mod, 
                            on='location_state', 
                            how="inner")

# Pasar fuelType a minusculas
df_joined_lowercase = df_joined.withColumn('fuelType', lower(car_rental_mod_rounded['fuelType']))

# Filtro 'Texas'
df_joined_lowercase_filtered = df_joined_lowercase.filter(df_joined_lowercase.state_name != 'Texas')

# Orden columnas
db_columns = [
    'fuelType',
     'rating',
     'renterTripsTaken',
     'reviewCount',
     'location_city',
     'state_name',
     'owner_id',
     'rate_daily',
     'vehicle_make',
     'vehicle_model',
     'vehicle_year'
]

# Columnas a castear para el datawarehouse
columns_to_cast = ['renterTripsTaken', 'reviewCount', 'owner_id', 'rate_daily', 'vehicle_year']

# Remueve columna no utilizada y ordena las columnas para el datawarehouse
df_final = df_joined_lowercase_filtered.drop('location_state') \
        .select(
            *[df_joined_lowercase_filtered[c].cast('int') if c in columns_to_cast else c for c in db_columns]
        )

# Ingesta al datawarehouse
df_final.write.insertInto('car_rental_db.car_rental_analytics')