# Solucion ejercitacion 3

De querer replicar el script en su contenedor, recuerde descargar la **imagen de hadoop** correspondiente al [Bootcamp de Data Engineering de EDVAI](https://www.escueladedatosvivos.ai/cursos/data-engineering-creando-el-pipeline-de-datos) y crear su contenedor segun lo indicado en el curso.

Pasos:
1. Correr el contenedor → `docker start <nombre contenedor>`
2. Ingrear al contenedor → `docker exec -it <nombre contenedor> bash`
3. Cambiar de usuario → `su hadoop`
4. Cambiar al directorio → `cd /home/hadoop/scripts`
5. Crear el script utilizando `nano landing.sh` y pegar el contenido del mismo dentro:

```
# Indica que interprete debe usar el script
#!/bin/bash

# Descarga el archivo a la carpeta landing
wget -P /home/hadoop/landing https://github.com/fpineyro/homework-0/blob/master/starwars.csv

# Mueve el archivo a HDFS Ingest
hdfs dfs -put /home/hadoop/landing/starwars.csv /ingest

# Borra el archivo del Landing
rm /home/hadoop/landing/starwars.csv
```

> El [script](https://github.com/EmanuelRodriguezBedeman/Data-Engineering-EDVAI/blob/main/Clase%203/landing.sh) puede ser encontrado en este mismo repositorio.

6. Cambiar permisos para que pueda ser ejecutado, leeido y pueda escribir → `chmod 700 landing.sh`.
7. Ejecutar el script → `/home/hadoop/scripts/landing.sh`
8. Ingresar la password del curso
9. Luego verificar que el archivo `starwars.csv` haya sido movido a la carpeta de *hdfs* utilizando → `hdfs dfs -ls /ingest`