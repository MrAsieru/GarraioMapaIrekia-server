#!/bin/sh
# Instalar PIP
sudo apt install python3-pip -y

# Instalar requirements.txt
sudo pip3 install -r ./requirements.txt -r ./api/requirements.txt

# Establecer variables de entorno
source ./mongodb/mongodb.env

# Iniciar servidor
docker compose up -d

# Descargar datos GTFS
python3 ./obtener_gtfs.py

# Generar archivos GeoJSON
python3 ./gtfs_to_geojson.py

# Generar archivos PMTiles y reiniciar servidor Martin
sh ./generar_tiles.sh

# Insertar datos en MongoDB
python3 ./gtfs_to_mongodb.py