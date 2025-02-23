# Solucion Ejercitacion 4

- [Consignas](https://github.com/EmanuelRodriguezBedeman/Data-Engineering-EDVAI/blob/main/Clase%204/Clase%204%20-%20Ejercicios%20Sqoop%20y%20Nifi%20-%20Alumnos.pdf)
- Soluciones:

## Ejercicios Sqoop

### **1)** Mostrar las tablas de la base de datos northwind

```
sqoop list-tables \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres -P
```

![Solucion ejercicio 1](imgs/image.png)

### **2)** Mostrar los clientes de Argentina

```
sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--m 1 \
--P \
--query "select company_name, contact_name from customers where country =  Argentina'"
```
![Solucion ejercicio 2](imgs/image-1.png)

### **3)** Importar un archivo .parquet que contenga toda la tabla orders. Luego ingestar el archivo a HDFS (*carpeta /sqoop/ingest*)

```
# Importa tabla orders como .parquet
sqoop import \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres\
    --table region\
    --m 1 \
    --P \
    --target-dir /sqoop/ingest \
    --as-parquetfile \
    --delete-target-dir
```

![Solucion Ejercicio 3, primera parte del print](imgs/image-2.png)

![Solucion Ejercicio 3, segunda parte del print](imgs/image-3.png)

#### Archivo .parquet en HDFS

![Solucion Ejercicio 3, archivo en HDFS](imgs/image-4.png)

###  **4)** Importar un archivo .parquet que contenga solo los productos con mas 20 unidades

```
sqoop import \
    --connect jdbc:postgresql://172.17.0.3:5432/northwind \
    --username postgres\
    --query "select product_id, product_name from products where units_in_stock > 20 AND \$CONDITIONS"\
    --m 1 \
    --P \
    --target-dir /sqoop/ingest \
    --as-parquetfile \
    --delete-target-dir
```

![Solucion Ejercicio 4, primer parte del print](imgs/image-5.png)

![Solucion Ejercicio 4, segunda parte del print](imgs/image-6.png)

![Solucion Ejercicio 4, tercera parte del print](imgs/image-7.png)

#### Archivo .parquet en HDFS

![Solucion Ejercicio 4, chequeando que este el archivo en HDFS](imgs/image-8.png)

---

## Ejercicios Nifi

### 1. Script de descarga starwars.csv

> ↓ **Script**: [ingest.sh](https://github.com/EmanuelRodriguezBedeman/Data-Engineering-EDVAI/blob/main/Clase%204/ingest.sh) ↓

```bash
# Indica que interprete debe usar el script
#!/bin/bash

# Descarga el archivo a la carpeta landing
wget -P /home/nifi/ingest https://github.com/fpineyro/homework-0/blob/master/starwars.csv
```

![Script dentro de ingest](imgs/image-11.png)

### 2. Procesos Nifi completo

![Proceso Nifi](imgs/image-9.png)

#### A. Toma archivo starwars.csv del directorio home/nifi/ingest

![getFile home/nifi/ingest](imgs/image-12.png)

#### B. Mover el archivo starwars.csv a home/nifi/bucket

![putFile home/nifi/bucket](imgs/image-13.png)

#### C. Tomar el archivo desde este ultimo directorio

![getFile home/nifi/bucket](imgs/image-14.png)

#### D. Ingestarlo en HDFS/nifi

![PutHDFS /nifi](imgs/image-15.png)

#### Arhivo starwars.parquet en hdfs/nifi

![Archivo starwars.parquet en hdfs/nifi](imgs/image-16.png)