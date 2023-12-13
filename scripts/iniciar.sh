#!/bin/bash
# Establecer variables de entorno
source ./mongodb/mongodb.env

# Descargar datos GTFS
python ./obtener_gtfs.py

# Insertar datos en MongoDB
python ./gtfs_to_mongodb.py

# Generar archivos GeoJSON
python ./gtfs_to_geojson.py

# Generar archivos PMTiles y reiniciar servidor Martin
sh ./generar_tiles.sh

# Calcular posiciones
python ./calcular_posiciones.py