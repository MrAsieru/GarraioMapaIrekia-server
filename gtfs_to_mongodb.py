import os
import json
import csv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

config = {}
directorio_gtfs = ""
directorio_geojson = ""

def conectar():
    # uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@127.0.0.1:27017/{os.environ['MONGODB_INITDB_DATABASE']}"
    uri = f"mongodb://serverUser:serverUser@192.168.1.10:27017/it0"
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente

def guardar(gtfs, cliente: MongoClient):
    agency_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "agency.txt"), ["agency_id"])
    route_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "routes.txt"), ["route_id"])
    stop_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "stops.txt"), ["stop_id"])

    # db = cliente[os.environ['MONGODB_INITDB_DATABASE']]
    db = cliente["it0"]
    colleccion_agency = db["agencies"]
    colleccion_rutas = db["routes"]
    colleccion_paradas = db["stops"]

    for agency_key in agency_list.keys():
        agency = agency_list[agency_key]
        doc = {
            "agency_id": agency.get("agency_id"),
            "name": agency.get("agency_name"),
            "url": agency.get("agency_url"),
            "timezone": agency.get("agency_timezone"),
            "lang": agency.get("agency_lang"),
            "phone": agency.get("agency_phone"),
            "email": agency.get("agency_email"),
            "fare_url": agency.get("agency_fare_url")
        }
        doc_id = colleccion_agency.insert_one(doc).inserted_id
        agency["mongodb_id"] = doc_id

    for route_key in route_list.keys():
        route = route_list[route_key]
        doc = {
            "route_id": route.get("route_id"),
            "agency": agency_list[route["agency_id"]].get("mongodb_id"),
            "name": {
                "short": route.get("route_short_name"),
                "long": route.get("route_long_name")
            },
            "desc": route.get("route_desc"),
            "type": route.get("route_type"),
            "url": route.get("route_url"),
            "color": {
                "color": route.get("route_color"),
                "text": route.get("route_text_color")
            },
            "sort_order": route.get("route_sort_order"),
            "network_id": route.get("network_id")
        }
        doc_id = colleccion_rutas.insert_one(doc).inserted_id
        route["mongodb_id"] = doc_id

    for stop_key in stop_list.keys():
        stop = stop_list[stop_key]
        doc = {
            "stop_id": stop.get("stop_id"),
            "code": stop.get("stop_code"),
            "name": stop.get("stop_name"),
            "desc": stop.get("stop_desc"),
            "coords": {
                "lat": stop.get("stop_lat"),
                "lon": stop.get("stop_lon")
            },
            "zone_id": stop.get("zone_id"),
            "url": stop.get("stop_url"),
            "location_type": stop.get("location_type"),
            "parent_station": stop.get("parent_station"),
            "stop_timezone": stop.get("stop_timezone"),
            "wheelchair_boarding": stop.get("wheelchair_boarding"),
            "level_id": stop.get("level_id"),
            "platform_code": stop.get("platform_code")
        }
        doc_id = colleccion_paradas.insert_one(doc).inserted_id
        stop["mongodb_id"] = doc_id


def csv_to_dict(archivo, primary_key: list):
    diccionario = {}
    with open(archivo, encoding="UTF-8") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            diccionario["_".join([fila[p] for p in primary_key])] = fila
    return diccionario


def main():
    global config, directorio_gtfs, directorio_geojson
    with open('config.json') as f:
        config = json.load(f)

    directorio_gtfs = os.path.join(os.getcwd(), config["directorio_gtfs"])

    feeds = []
    with open(os.path.join(os.getcwd(), config["feeds"])) as f:
        feeds = json.load(f)

    cliente = conectar()
    # Limpiar base de datos
    # for colleccion in cliente[os.environ['MONGODB_INITDB_DATABASE']].list_collection_names():
    for colleccion in cliente["it0"].list_collection_names():
        # cliente[os.environ['MONGODB_INITDB_DATABASE']].drop_collection(colleccion)
        cliente["it0"].drop_collection(colleccion)

    for feed in feeds:
        guardar(feed, cliente)


if __name__ == '__main__':
    main()