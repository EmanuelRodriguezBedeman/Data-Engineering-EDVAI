# Import librerias y creacion de sesion en Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum

spark = SparkSession.builder \
    .appName("F1") \
    .enableHiveSupport() \
    .getOrCreate()

# Carga de datos
constructors = spark.read.option('header', 'true').csv('hdfs://172.17.0.2:9000/ingest/constructors.csv')
drivers = spark.read.option('header', 'true').csv('hdfs://172.17.0.2:9000/ingest/drivers.csv')
races = spark.read.option('header', 'true').csv('hdfs://172.17.0.2:9000/ingest/races.csv')
results = spark.read.option('header', 'true').csv('hdfs://172.17.0.2:9000/ingest/results.csv')

# Seleccion y casting de columnas necesarias
drivers_mod = drivers.select(drivers.driverId.cast("int"), drivers.forename, drivers.surname, drivers.nationality)

races_mod = races.select(races["raceId"].cast("int"), races["year"].cast("date"), "name")

constructors_mod = constructors.select(constructors['constructorId'].cast("int"), 'constructorRef', 'name', 'nationality', 'url')

results_mod = results.select(results["resultId"].cast("int"), results["raceId"].cast("int"), results["driverId"].cast("int"), results["constructorId"].cast("int"), results["points"].cast("int"))

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Insertar en la tabla driver_results los corredores con mayor cantidad de puntos en la historia.

# Seleccion columnas driverId y points de la tabla results
results_drivers = results_mod.select('driverId', 'points')

# Union tabla results & drivers
driver_results = drivers_mod.join(results_drivers, how="inner", on="driverId")

# Agrupacion segun conductores ordenado segun puntos
top_drivers = driver_results.groupby("forename", "surname", "nationality").agg({"points":"sum"}).orderBy("sum(points)", ascending=False)

# Inserta los datos en la BD f1, tabla 'drver_results'
top_drivers.write.insertInto("f1.driver_results")

# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Insertar en la tabla constructor_result quienes obtuvieron más puntos en el Spanish Grand Prix en el año 1991.

# Obtencion de raceId del Spanish Grand Prix de 1991
race_91 = races_mod.filter((races_mod['name'] == "Spanish Grand Prix") & (year(races_mod['year']) == 1991)).select('raceId')

# Seleccion columnas de resultados
results_cons = results_mod.select('raceId', 'constructorId', 'points')

# Union tabla results con raceId del spanish grand prix del 91 y constructores
result_91 = results_cons.join(race_91, on="raceId", how="inner").join(constructors_mod, on="constructorId", how="inner").drop("raceId", "constructorId")

# Agrupacion por constructor y sus puntos en el Spanish Gran Prix de 1991
constructors_results = result_91.groupby("constructorRef", "name", "nationality", "url").agg({"points":"sum"}).orderBy("sum(points)", ascending=False)

# Inserta resultados en la BD f1, en la tabla constructor_results
constructors_results.write.insertInto("f1.constructor_results")