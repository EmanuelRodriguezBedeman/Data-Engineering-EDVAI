# Solucion Ejercicios Clase 4 

- Archivo con las consignas
- Consigna con solucion:

## Ejercicios Sqoop

### **1)** Mostrar las tablas de la base de datos northwind

```
sqoop list-tables \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres -P
```

![Solucion ejercicio 1](image.png)

### **2)** Mostrar los clientes de Argentina

```
sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--m 1 \
--P \
--query "select company_name, contact_name from customers where country =  Argentina'"
```
![Solucion ejercicio 2](image-1.png)

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

###  **4)** Importar un archivo .parquet que contenga solo los productos con mas 20 unidades