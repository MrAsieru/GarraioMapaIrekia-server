import os
import json
import csv
from pathlib import Path
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.typings import _DocumentType

config = {}
directorio_gtfs = ""
directorio_geojson = ""

def conectar():
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        # Prod
        uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@127.0.0.1:27017/{os.environ['MONGODB_INITDB_DATABASE']}"
    else:
        #TODO: Quitar (Solo para pruebas)
        uri = f"mongodb://serverUser:serverUser@192.168.1.10:27017/gtfs"
    
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente

def guardar(gtfs, db: Database[_DocumentType]):
    agency_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "agency.txt"), ["agency_id"])
    route_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "routes.txt"), ["route_id"])
    stop_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["id"], "stops.txt"), ["stop_id"])
    
    colleccion_agency = db["agencies"]
    colleccion_rutas = db["routes"]
    colleccion_paradas = db["stops"]

    for agency_key in agency_list.keys():
        agency: dict = agency_list[agency_key]
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
        route: dict = route_list[route_key]
        doc = {
            "route_id": route.get("route_id"),
            "agency_id": route.get("agency_id"),
            "agency": agency_list[route["agency_id"]].get("mongodb_id"),
            "short_name": route.get("route_short_name"),
            "long_name": route.get("route_long_name"),
            "desc": route.get("route_desc"),
            "type": route.get("route_type"),
            "url": route.get("route_url"),
            "color": route.get("route_color") if route.get("route_color", "").startswith('#') else "#"+route.get("route_color", ""),
            "text_color": route.get("route_text_color") if route.get("route_text_color", "").startswith('#') else "#"+route.get("route_text_color", ""),
            "sort_order": route.get("route_sort_order"),
            "network_id": route.get("network_id")
        }
        doc_id = colleccion_rutas.insert_one(doc).inserted_id
        route["mongodb_id"] = doc_id

    for stop_key in stop_list.keys():
        stop: dict = stop_list[stop_key]
        doc = {
            "stop_id": stop.get("stop_id"),
            "code": stop.get("stop_code"),
            "name": stop.get("stop_name"),
            "desc": stop.get("stop_desc"),
            "coords_lat": stop.get("stop_lat"),
            "coords_lon": stop.get("stop_lon"),
            "zone_id": stop.get("zone_id"),
            "url": stop.get("stop_url"),
            "location_type": stop.get("location_type") if stop.get("location_type", "") != "" else "0",
            "parent_station": stop.get("parent_station"),
            "timezone": stop.get("stop_timezone"),
            "wheelchair_boarding": stop.get("wheelchair_boarding"),
            "level_id": stop.get("level_id"),
            "platform_code": stop.get("platform_code")
        }
        doc_id = colleccion_paradas.insert_one(doc).inserted_id
        stop["mongodb_id"] = doc_id

    db["feeds"].update_one({"id": gtfs["id"]}, {"$set": {"actualizar": False}})


def csv_to_dict(archivo, primary_key: list) -> dict:
    diccionario = {}
    with open(archivo, encoding="UTF-8") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            diccionario["_".join([fila[p] for p in primary_key])] = fila
    return diccionario


def main():
    global config, directorio_gtfs, directorio_geojson
    load_dotenv(dotenv_path=Path('./mongodb/mongodb.env'))
    with open('config.json') as f:
        config = json.load(f)

    directorio_gtfs = os.path.join(os.getcwd(), config["directorio_gtfs"])

    cliente = conectar()
    # Limpiar base de datos
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        db = cliente[os.environ['MONGODB_INITDB_DATABASE']]
    else:
        #TODO: Quitar (Solo para pruebas)
        db = cliente["gtfs"]

    # Eliminar documentos que contengan id con el prefijo de los feeds que se deben actualizar
    feeds_actualizar_ids = db["feeds"].distinct("id", {"actualizar": True})
    for colleccion in ["agencies", "routes", "stops"]:
        db[colleccion].delete_many({"id": {"$regex": f"^{('|'.join(feeds_actualizar_ids))}_"}})

    for feed in db["feeds"].find({"actualizar": True}):
        guardar(feed, db)


if __name__ == '__main__':
    main()