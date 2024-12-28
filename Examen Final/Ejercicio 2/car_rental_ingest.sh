#!/bin/bash

# Mensaje de inicio
echo "****** Inicio Ingesta Alquiler Automoviles ******"

# Directorio landing en hadoop
LANDING_DIR="/home/hadoop/landing"

# Directorio destino en HDFS
DEST_DIR="/ingest"

# Link al bucket
LINK="https://dataengineerpublic.blob.core.windows.net/data-engineer/"

# Nombre archivos
CAR_RENTAL="CarRentalData.csv"
GEOREF="georef-united-states-of-america-state.csv"

# Descarga archivos
wget -P $LANDING_DIR $LINK$CAR_RENTAL
wget -P $LANDING_DIR -O $LANDING_DI/$GEOREF $LINK$GEOREF

# Mover archivos a HDFS
hdfs dfs -put $LANDING_DIR/$CAR_RENTAL $DEST_DIR
hdfs dfs -put $LANDING_DIR/$GEOREF $DEST_DIR

# Remueve archivos
rm $LANDING_DIR/$CAR_RENTAL
rm $LANDING_DIR/$GEOREF

# Mensaje de finalizacion
echo "\n****** Fin Ingesta Alquiler Automoviles ******"