import urllib.request
import zipfile
import os
import json
import csv
from typing import List

config = {}
directorio_gtfs = ""
directorio_geojson = ""


def generar(gtfs):
    geojson = {
        "type": "FeatureCollection",
        "bbox": [],
        "features": []
    }

    # Transformar stops.txt a un diccionario
    stops_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "stops.txt"))

    # Incializar bbox con la primera coordenada
    print(gtfs["id"])
    bbox = [round(float(stops_list[0]["stop_lon"]), 5), round(float(stops_list[0]["stop_lat"]), 5), round(float(stops_list[0]["stop_lon"]), 5), round(float(stops_list[0]["stop_lat"]), 5)]

    # Guardar paradas
    for stop in stops_list:
        lon = round(float(stop["stop_lon"]), 5) # Redondear a 5 decimales para ahorrar espacio
        lat = round(float(stop["stop_lat"]), 5)

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "stop_id": stop["stop_id"],
                "stop_name": stop.get("stop_name"),
                "zone_id": stop.get("zone_id"),
                "location_type": stop.get("location_type") if stop.get("location_type", "") != "" else "0",
                "parent_station": stop.get("parent_station"),
                "platform_code": stop.get("platform_code")
            },
            "tippecanoe" : { "layer" : "paradas" } # Establecer layer para tippecanoe
        }

        # Actualizar bbox
        if lon < bbox[0]: # minLon
            bbox[0] = lon
        elif lon > bbox[2]: # maxLon
            bbox[2] = lon
        if lat < bbox[1]: # minLat
            bbox[1] = lat
        elif lat > bbox[3]: # maxLat
            bbox[3] = lat

        geojson["features"].append(feature)

    # Comprobar si existe shapes.txt
    if os.path.isfile(os.path.join(directorio_gtfs, gtfs["id"], "shapes.txt")):
        # Transformar routes.txt, trips.txt y shapes.txt a un diccionario
        route_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "routes.txt"))
        trip_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "trips.txt"))
        shape_list = sorted(csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "shapes.txt")), key=lambda k: int(k['shape_pt_sequence']))
        shape_dict = {}

        # Crear un diccionario con los shapes y sus coordenadas
        for shape in shape_list:
            if shape["shape_id"] not in shape_dict.keys():
                shape_dict[shape["shape_id"]] = []
            lon = round(float(shape["shape_pt_lon"]), 5) # Redondear a 5 decimales para ahorrar espacio
            lat = round(float(shape["shape_pt_lat"]), 5)
            shape_dict[shape["shape_id"]].append([lon, lat])

            # Actualizar bbox
            if lon < bbox[0]: # minLon
                bbox[0] = lon
            elif lon > bbox[2]: # maxLon
                bbox[2] = lon
            if lat < bbox[1]: # minLat
                bbox[1] = lat
            elif lat > bbox[3]: # maxLat
                bbox[3] = lat

        # Agregar los shapes a geojson
        for shape_id in shape_dict.keys():
            # Obtener las rutas a partir de los viajes que usan el shape
            routes = [y for y in route_list if y["route_id"] in [x["route_id"] for x in trip_list if x["shape_id"] == shape_id]]
            route_ids = set([route["route_id"] for route in routes])

            # Crear feature (en caso de que el shape tenga más de una ruta asociada, se creará una feature por cada una)
            for route_id in route_ids:
                route = [x for x in routes if x["route_id"] == route_id][0]
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": shape_dict[shape_id]
                    },
                    "properties": {
                        "shape_id": shape_id,
                        "route_id": route_id,
                        "agency_id": route["agency_id"],
                        "long_name": route.get("route_long_name"),
                        "short_name": route.get("route_short_name"),
                        "route_type": route["route_type"]
                    },
                    "tippecanoe" : { "layer" : "lineas" } # Establecer layer para tippecanoe
                }

                if route.get("route_color"):
                    feature["properties"]["route_color"] = route["route_color"] if route["route_color"][0] == '#' else "#"+route["route_color"]
                if route.get("route_text_color"):
                    feature["properties"]["route_text_color"] = route["route_text_color"] if route["route_text_color"][0] == '#' else "#"+route["route_text_color"]
                
                geojson["features"].append(feature)

    # Guardar bbox final
    geojson["bbox"] = bbox

    # write file
    with open(os.path.join(directorio_geojson, gtfs["id"]+".geojson"), 'w') as outfile:
        json.dump(geojson, outfile)


def csv_to_dict(archivo) -> List[dict]:
    lista = []
    with open(archivo, encoding="UTF-8") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            lista.append(fila)
    return lista


def main():
    global config, directorio_gtfs, directorio_geojson
    with open('config.json') as f:
        config = json.load(f)

    directorio_gtfs = os.path.join(os.getcwd(), config["directorio_gtfs"])
    directorio_geojson = os.path.join(os.getcwd(), config["directorio_geojson"])

    try:
        os.mkdir(directorio_geojson)
    except FileExistsError:
        pass

    feeds = []
    with open(os.path.join(os.getcwd(), config["feeds"])) as f:
        feeds = json.load(f)

    for feed in feeds:
        generar(feed)


if __name__ == '__main__':
    main()