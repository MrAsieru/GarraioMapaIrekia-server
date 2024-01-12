#!/bin/bash
lockfile=/tmp/iniciar.lock

(
    if ! flock -n 200; then
        echo "El script ya está ejecutandose" >&2
        exit 1
    fi

    # Descargar datos GTFS
    python -u /server/scripts/obtener_gtfs.py

    # Insertar datos en MongoDB
    python -u /server/scripts/gtfs_to_mongodb.py

    # Generar archivos GeoJSON
    python -u /server/scripts/gtfs_to_geojson.py

    # Generar archivos PMTiles
    tippecanoe -z18 -B 10 -f -o /server/tiles/tiles.pmtiles /server/geojson/*.geojson

    # Reiniciar servidor Martin
    docker restart martin

    # Calcular posiciones
    python -u /server/scripts/calcular_posiciones.py
) 200>${lockfile}