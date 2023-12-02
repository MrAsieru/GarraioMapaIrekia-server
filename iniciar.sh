#!/bin/sh
# Establecer variables de entorno
source ./mongodb/mongodb.env

# Iniciar servidor
docker compose up -d --build

# Descargar datos GTFS
python3 ./obtener_gtfs.py

# Insertar datos en MongoDB
python3 ./gtfs_to_mongodb.py

# Generar archivos GeoJSON
python3 ./gtfs_to_geojson.py

# Generar archivos PMTiles y reiniciar servidor Martin
sh ./generar_tiles.sh

# Calcular posiciones
python3 ./calcular_posiciones.py