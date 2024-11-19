# Resolucion ejercicios

## 1. En Hive, crear las siguientes tablas (internas) en la base de datos `tripdata` en hive

* `payments(VendorID, tpep_pickup_datetime, payment_type, total_amount)`

```
CREATE TABLE payments(VendorID int, tpep_pickup_datetime date, payment_type STRING, total_amount float)
COMMENT "Table 'payments' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```

![Creacion tabla 'payments' en Hive](image.png)

* `passengers(tpep_pickup_datetime, passenger_count, total_amount)`

```
CREATE TABLE passengers(tpep_pickup_datetime date, passenger_count int, total_amount float)
COMMENT "Table 'passengers' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```

![Creacion tabla 'passengers' en Hive](image-1.png)

* `tolls(tpep_pickup_datetime, passenger_count, tolls_amount, total_amount)`

```
CREATE TABLE tolls(tpep_pickup_datetime date, passenger_count int, tolls_amount float, total_amount float)
COMMENT "Table 'tolls' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```

![Creacion tabla 'tolls' en Hive](image-2.png)

* `congestion(tpep_pickup_datetime, passenger_count, congestion_surcharge, total_amount)`

```
CREATE TABLE congestion(tpep_pickup_datetime date, passenger_count int, congestion_surcharge float, total_amount float)
COMMENT "Table 'congestion' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```

![Creacion tabla 'congestion' en Hive](image-3.png)

* `distance(tpep_pickup_datetime, passenger_count, trip_distance, total_amount)`

```
CREATE TABLE distance(tpep_pickup_datetime date, passenger_count int, trip_distance float, total_amount float)
COMMENT "Table 'distance' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```

![Creacion tabla 'distance' en Hive](image-4.png)

---

## 2. En Hive, hacer un ‘describe’ de las tablas passengers y distance

* Tabla 'passengers'

```
DESCRIBE passengers;
```

![Describe tabla 'passengers'](image-5.png)

* Tabla 'distance'

```
DESCRIBE distance;
```

![Describe tabla 'distance'](image-6.png)

---

## 3. Hacer ingest del file: *yellow_tripdata_2021-01.csv*

HDFS

```
wget https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.csv
```

---

## 5. Insertar en la tabla payments (VendorID, tpep_pickup_datetime, payment_type, total_amount). Solamente los pagos con tarjeta de crédito.

```
df = spark.read.option('header', 'true').csv('/ingest/yellow_tripdata_2021-01.csv')
df_payments = df.select(df.VendorID.cast('int'), df.tpep_pickup_datetime.cast('date'), df.payment_type.cast('string'), df.total_amount.cast('float'))
df_payments.show(5)
df_payments_filtered = df_payments.filter(df_payments.payment_type == 1)
df_payments_filtered.show(5)
df_payments_filtered.write.insertInto('tripdata.payments')
```

![Select & Insert tabla 'payments'](image-7.png)

![Print tabla 'payments' en Hive](image-8.png)

---

## 6. Insertar en la tabla passengers (tpep_pickup_datetime, passenger_count, total_amount) los registros cuya cantidad de pasajeros sea mayor a 2 y el total del viaje cueste más de 8 dólares.

```
df = spark.read.option('header', 'true').csv('/ingest/yellow_tripdata_2021-01.csv')
df_passengers = df.select(df.tpep_pickup_datetime.cast('date'), df.passenger_count.cast('int'), df.total_amount.cast('float'))
df_passengers.show(5)
df_passengers_filtered = df_passengers.filter((df_passengers.passenger_count > 2) & (df_passengers.total_amount > 8))
df_passengers_filtered.show(5)
df_passengers_filtered.write.insertInto('tripdata.passengers')
```

![Select & Insert tabla 'passengers'](image-9.png)

![Print tabla 'passengers' en Hive](image-10.png)

---

## 7. Insertar en la tabla tolls (tpep_pickup_datetime, passenger_count, tolls_amount, total_amount) los registros que tengan pago de peajes mayores a 0.1 y cantidad de pasajeros mayores a 1.

```
df = spark.read.option('header', 'true').csv('/ingest/yellow_tripdata_2021-01.csv')
df_tolls = df.select(df.tpep_pickup_datetime.cast('date'), df.passenger_count.cast('int'), df.tolls_amount.cast('float'), df.total_amount.cast('float'))
df_tolls.show(5)
df_tolls_filtered = df_tolls.filter((df_tolls.passenger_count > 1) & (df_tolls.tolls_amount > 0.1))
df_tolls_filtered.show(5)
df_tolls_filtered.write.insertInto('tripdata.tolls')
```

![Select & Insert tabla 'tolls'](image-11.png)

![Print tabla 'tolls' en Hive](image-12.png)

---

## 8. Insertar en la tabla congestion (tpep_pickup_datetime, passenger_count congestion_surcharge, total_amount) los registros que hayan tenido congestión en los viajes en la fecha 2021-01-18

```
df = spark.read.option('header', 'true').csv('/ingest/yellow_tripdata_2021-01.csv')
df_congestion = df.select(df.tpep_pickup_datetime.cast('date'), df.passenger_count.cast('int'), df.congestion_surcharge.cast('float'), df.total_amount.cast('float'))
df_congestion.show(5)
df_congestion_filtered = df_congestion.filter(df_congestion.tpep_pickup_datetime == 2021-01-18)
df_congestion_filtered.show(5)
df_congestion_filtered.write.insertInto('tripdata.congestion')
```

![Select & Insert tabla 'congestion'](image-13.png)

![Print tabla 'congestion' en Hive](image-14.png)

---