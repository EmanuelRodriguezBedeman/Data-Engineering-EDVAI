## Resolucion ejercicios

1. En Hive, crear las siguientes tablas (internas) en la base de datos tripdata en hive

* `payments(VendorID, tpep_pickup_datetetime, payment_type, total_amount)`

```
CREATE TABLE payments(VendorID int, tpep_pickup_datetetime date, payment_type STRING, total_amount float)
COMMENT "Table 'payments' for bootcamp exercise 6"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/tables/external/tripsdb';
```

![Creacion tabal Payments en Hive](imgs/image.png)