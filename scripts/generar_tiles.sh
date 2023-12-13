#!/bin/sh
# Genera archivos tiles de todos los geojson
tippecanoe -z18 -B 10 -f -o tiles/tiles.pmtiles geojson/*.geojson

# Reiniciar servidor Martin
docker compose restart martin