# Resolucion ejercicios

## 1. En Hive, crear las siguientes tablas (internas) en la base de datos `tripdata` en hive

* `payments(VendorID, tpep_pickup_datetetime, payment_type, total_amount)`

```
CREATE TABLE payments(VendorID int, tpep_pickup_datetetime date, payment_type STRING, total_amount float)
COMMENT "Table 'payments' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/tripsdb';
```

![Creacion tabla 'payments' en Hive](imgs/image.png)

* `passengers(tpep_pickup_datetetime, passenger_count, total_amount)`

```
CREATE TABLE passengers(tpep_pickup_datetetime date, passenger_count int, total_amount float)
COMMENT "Table 'passengers' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/tripsdb';
```

![Creacion tabla 'passengers' en Hive](imgs/image1.png)

* `tolls(tpep_pickup_datetetime, passenger_count, tolls_amount, total_amount)`

```
CREATE TABLE tolls(tpep_pickup_datetetime date, passenger_count int, tolls_amount float, total_amount float)
COMMENT "Table 'tolls' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/tripsdb';
```

![Creacion tabla 'tolls' en Hive](imgs/image2.png)

* `congestion(tpep_pickup_datetetime, passenger_count, congestion_surcharge, total_amount)`

```
CREATE TABLE congestion(tpep_pickup_datetetime date, passenger_count int, congestion_surcharge float, total_amount float)
COMMENT "Table 'congestion' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/tripsdb';
```

![Creacion tabla 'congestion' en Hive](imgs/image3.png)

* `distance(tpep_pickup_datetetime, passenger_count, trip_distance, total_amount)`

```
CREATE TABLE distance(tpep_pickup_datetetime date, passenger_count int, trip_distance float, total_amount float)
COMMENT "Table 'distance' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/tripsdb';
```

![Creacion tabla 'distance' en Hive](imgs/image4.png)


## 2. En Hive, hacer un ‘describe’ de las tablas passengers y distance

* Tabla 'passengers'

```
DESCRIBE passengers;
```

![Descripcion tabla 'passengers'](imgs/image5.png)

* Tabla 'distance'

```
DESCRIBE distance;
```

![Descripcion tabla 'distance'](imgs/image6.png)