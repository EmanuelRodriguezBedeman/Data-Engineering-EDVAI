# Indica que interprete debe usar el script
#!/bin/bash

# Descarga el archivo a la carpeta landing
wget -P /home/hadoop/landing https://github.com/fpineyro/homework-0/blob/master/starwars.csv

# Mueve el archivo a HDFS Ingest
hdfs dfs -put /home/hadoop/landing/starwars.csv /ingest

# Borra el archivo del Landing
rm /home/hadoop/landing/starwars.csv