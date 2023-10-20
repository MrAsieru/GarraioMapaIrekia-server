from pathlib import Path
import urllib.request
import zipfile
import os
import json
import csv
from typing import List

from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

config = {}
directorio_gtfs = ""
directorio_geojson = ""


def conectar() -> MongoClient:
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        # Prod
        uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@127.0.0.1:27017/{os.environ['MONGODB_INITDB_DATABASE']}"
    else:
        #TODO: Quitar (Solo para pruebas)
        uri = f"mongodb://serverUser:serverUser@192.168.1.10:27017/gtfs"
    
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente


def generar(gtfs):
    geojson = {
        "type": "FeatureCollection",
        "bbox": [],
        "features": []
    }

    # Transformar stops.txt a un diccionario
    stops_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "stops.txt"))

    # Incializar bbox con la primera coordenada
    print(gtfs["idFeed"])
    bbox = [round(float(stops_list[0]["stop_lon"]), 5), round(float(stops_list[0]["stop_lat"]), 5), round(float(stops_list[0]["stop_lon"]), 5), round(float(stops_list[0]["stop_lat"]), 5)]

    # Guardar paradas
    for stop in stops_list:
        lon = round(float(stop.get("stop_lon")), 5) # Redondear a 5 decimales para ahorrar espacio
        lat = round(float(stop.get("stop_lat")), 5)

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "stop_id": stop["stop_id"],
                "name": stop.get("stop_name"),
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
    if os.path.isfile(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt")):
        # Transformar routes.txt, trips.txt y shapes.txt a un diccionario
        route_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "routes.txt"))
        trip_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "trips.txt"))
        shape_list = sorted(csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt")), key=lambda k: int(k['shape_pt_sequence']))
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
            routes = [r for r in route_list if r["route_id"] in [t["route_id"] for t in trip_list if t.get("shape_id") == shape_id]]
            route_ids = set([route["route_id"] for route in routes])

            # Crear feature (en caso de que el shape tenga más de una ruta asociada, se creará una feature por cada una)
            for route_id in route_ids:
                route = [r for r in routes if r["route_id"] == route_id][0]
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
                        "type": route["route_type"],
                        "color": route.get("route_color") if route.get("route_color", "").startswith('#') else "#"+route.get("route_color", ""),
                        "text_color": route.get("route_text_color") if route.get("route_text_color", "").startswith('#') else "#"+route.get("route_text_color", "")
                    },
                    "tippecanoe" : { "layer" : "lineas" } # Establecer layer para tippecanoe
                }

                geojson["features"].append(feature)

    # Guardar bbox final
    geojson["bbox"] = bbox

    # write file
    with open(os.path.join(directorio_geojson, gtfs["idFeed"]+".geojson"), 'w') as outfile:
        json.dump(geojson, outfile)


def csv_to_dict(archivo) -> List[dict]:
    lista = []
    with open(archivo, encoding="utf-8-sig") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            lista.append(fila)
    return lista


def main():
    global config, directorio_gtfs, directorio_geojson
    load_dotenv(dotenv_path=Path('./mongodb/mongodb.env'))
    with open('config.json') as f:
        config = json.load(f)

    directorio_gtfs = os.path.join(os.getcwd(), config["directorio_gtfs"])
    directorio_geojson = os.path.join(os.getcwd(), config["directorio_geojson"])

    try:
        os.mkdir(directorio_geojson)
    except FileExistsError:
        pass

    cliente = conectar()
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        db = cliente[os.environ['MONGODB_INITDB_DATABASE']]
    else:
        db = cliente["gtfs"]

    for feed in db["feeds"].find({"actualizar": True}):
        generar(feed)


if __name__ == '__main__':
    main()