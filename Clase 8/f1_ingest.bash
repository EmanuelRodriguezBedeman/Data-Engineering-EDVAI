#!/bin/bash

# Mensaje de inicio
echo "****** Started Ingest F1 ******"

# Variable directorio landing
LANDING_DIR="/home/hadoop/landing"

# Array con nombre de archivos
ARCHIVOS=("results.csv" "drivers.csv" "constructors.csv" "races.csv")

# Loop para realizar ingesta
for ARCHIVO in "${ARCHIVOS[@]}"; do
    wget -P $LANDING_DIR https://dataengineerpublic.blob.core.windows.net/data-engineer/f1/$ARCHIVO

    hdfs dfs -put $LANDING_DIR/$ARCHIVO /ingest

    rm $LANDING_DIR/$ARCHIVO
done

# Mensaje de fin
echo "\n****** End Ingest F1 ******"