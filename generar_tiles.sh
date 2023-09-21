#!/bin/sh
# Genera archivos tiles de cada geojson
for file in geojson/*.geojson; do
    filename=$(basename "$file")
    filename="${filename%.*}"
    tippecanoe -z18 -B 10 -f -o tiles/"$filename".pmtiles "$file"
done

# Reiniciar servidor Martin
sudo docker restart martin