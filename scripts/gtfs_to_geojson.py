from datetime import datetime
from pathlib import Path
import sys
import os
import json
import csv
from typing import List
from pymongo import UpdateOne
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
from pymongo.typings import _DocumentType

config = {}
directorio_gtfs = ""
directorio_geojson = ""


def conectar() -> MongoClient:
    uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@mongodb:27017/gtfs"
    
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente


def generar(gtfs, db_paradas: Collection[_DocumentType], db_lineas: Collection[_DocumentType], db_agencias: Collection[_DocumentType], db_viajes: Collection[_DocumentType] = None):
    geojson = {
        "type": "FeatureCollection",
        "bbox": [],
        "features": []
    }

    # Transformar stops.txt a un diccionario
    stops_list = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "stops.txt"))

    # Incializar bbox con la primera coordenada
    print(gtfs["idFeed"])
    bbox = [round(float(stops_list[0]["stop_lon"]), 5), round(float(stops_list[0]["stop_lat"]), 5), round(float(stops_list[0]["stop_lon"]), 5), round(float(stops_list[0]["stop_lat"]), 5)]
    agencias_bbox = {} # idAgencia: [minLon, minLat, maxLon, maxLat]

    # Guardar paradas
    for stop in stops_list:
        agencias = db_paradas.find_one({"_id": stop["stop_id"]}, {"agencias": 1}).get("agencias", [])
        
        lon = round(float(stop.get("stop_lon")), 5) # Redondear a 5 decimales para ahorrar espacio
        lat = round(float(stop.get("stop_lat")), 5)

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": {
                "idParada": stop["stop_id"],
                "nombre": stop.get("stop_name"),
                "idZona": stop.get("zone_id"),
                "tipo": stop.get("location_type") if stop.get("location_type", "") != "" else "0",
                "paradaPadre": stop.get("parent_station"),
                "codigoPlataforma": stop.get("platform_code"),
                "agencias": agencias
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

        # Actualizar bbox (agencia)
        for agencia in agencias:
            if agencia not in agencias_bbox.keys():
                agencias_bbox[agencia] = [lon, lat, lon, lat]
            else:
                if lon < agencias_bbox[agencia][0]:
                    agencias_bbox[agencia][0] = lon
                elif lon > agencias_bbox[agencia][2]:
                    agencias_bbox[agencia][2] = lon
                if lat < agencias_bbox[agencia][1]:
                    agencias_bbox[agencia][1] = lat
                elif lat > agencias_bbox[agencia][3]:
                    agencias_bbox[agencia][3] = lat

        geojson["features"].append(feature)

    # Comprobar si existe shapes.txt
    lineas_bbox = {} # idLinea: [minLon, minLat, maxLon, maxLat]
    viajes_bbox = {} # idViaje: [minLon, minLat, maxLon, maxLat]

    trip_list = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "trips.txt"))
    if os.path.isfile(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt")):
        # Transformar routes.txt, trips.txt y shapes.txt a un diccionario
        route_list = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "routes.txt"))
        shape_list = sorted(csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt")), key=lambda k: int(k['shape_pt_sequence']))
        agency_list = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "agency.txt"), ["agency_id"])
        shape_dict = {}

        shapes_bbox = {} # idShape: [minLon, minLat, maxLon, maxLat]

        # Crear un diccionario con los shapes y sus coordenadas
        for shape in shape_list:
            if shape["shape_id"] not in shape_dict.keys():
                shape_dict[shape["shape_id"]] = []
            lon = round(float(shape["shape_pt_lon"]), 5) # Redondear a 5 decimales para ahorrar espacio
            lat = round(float(shape["shape_pt_lat"]), 5)
            shape_dict[shape["shape_id"]].append([lon, lat])

            # Actualizar bbox (shapes)
            if shape["shape_id"] not in shapes_bbox.keys():
                shapes_bbox[shape["shape_id"]] = [lon, lat, lon, lat]
            else:
                if lon < shapes_bbox[shape["shape_id"]][0]:
                    shapes_bbox[shape["shape_id"]][0] = lon
                elif lon > shapes_bbox[shape["shape_id"]][2]:
                    shapes_bbox[shape["shape_id"]][2] = lon
                if lat < shapes_bbox[shape["shape_id"]][1]:
                    shapes_bbox[shape["shape_id"]][1] = lat
                elif lat > shapes_bbox[shape["shape_id"]][3]:
                    shapes_bbox[shape["shape_id"]][3] = lat

            # Actualizar bbox (feed)
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
                # Actualizar BBOX de la linea
                if route_id not in lineas_bbox.keys():
                    lineas_bbox[route_id] = [shapes_bbox[shape_id][0], shapes_bbox[shape_id][1], shapes_bbox[shape_id][2], shapes_bbox[shape_id][3]]
                else:
                    if shapes_bbox[shape_id][0] < lineas_bbox[route_id][0]:
                        lineas_bbox[route_id][0] = shapes_bbox[shape_id][0]
                    elif shapes_bbox[shape_id][2] > lineas_bbox[route_id][2]:
                        lineas_bbox[route_id][2] = shapes_bbox[shape_id][2]
                    if shapes_bbox[shape_id][1] < lineas_bbox[route_id][1]:
                        lineas_bbox[route_id][1] = shapes_bbox[shape_id][1]
                    elif shapes_bbox[shape_id][3] > lineas_bbox[route_id][3]:
                        lineas_bbox[route_id][3] = shapes_bbox[shape_id][3]

                route = [r for r in routes if r["route_id"] == route_id][0]

                # Actualizar BBOX de la agencia
                if route["agency_id"] not in agencias_bbox.keys():
                    agencias_bbox[route["agency_id"]] = [shapes_bbox[shape_id][0], shapes_bbox[shape_id][1], shapes_bbox[shape_id][2], shapes_bbox[shape_id][3]]
                else:
                    if shapes_bbox[shape_id][0] < agencias_bbox[route["agency_id"]][0]:
                        agencias_bbox[route["agency_id"]][0] = shapes_bbox[shape_id][0]
                    elif shapes_bbox[shape_id][2] > agencias_bbox[route["agency_id"]][2]:
                        agencias_bbox[route["agency_id"]][2] = shapes_bbox[shape_id][2]
                    if shapes_bbox[shape_id][1] < agencias_bbox[route["agency_id"]][1]:
                        agencias_bbox[route["agency_id"]][1] = shapes_bbox[shape_id][1]
                    elif shapes_bbox[shape_id][3] > agencias_bbox[route["agency_id"]][3]:
                        agencias_bbox[route["agency_id"]][3] = shapes_bbox[shape_id][3]
                
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": shape_dict[shape_id]
                    },
                    "properties": {
                        "idRecorrido": shape_id,
                        "idLinea": route_id,
                        "idAgencia": route["agency_id"],
                        "nombreAgencia": agency_list[route["agency_id"]]["agency_name"],
                        "nombreLargo": route.get("route_long_name"),
                        "nombreCorto": route.get("route_short_name"),
                        "orden": route.get("route_sort_order") if route.get("route_sort_order", "") != "" else "0",
                        "tipo": route["route_type"],
                        "color": route.get("route_color") if route.get("route_color", "").startswith('#') else "#"+route.get("route_color", ""),
                        "colorTexto": route.get("route_text_color") if route.get("route_text_color", "").startswith('#') else "#"+route.get("route_text_color", ""),
                        "bbox": shapes_bbox[shape_id]
                    },
                    "tippecanoe" : { "layer" : "lineas" } # Establecer layer para tippecanoe
                }

                geojson["features"].append(feature)

            # Actualizar BBOX de los viajes
            for viaje in [t for t in trip_list if t.get("shape_id") == shape_id]:
                if viaje["trip_id"] not in viajes_bbox.keys():
                    viajes_bbox[viaje["trip_id"]] = [shapes_bbox[shape_id][0], shapes_bbox[shape_id][1], shapes_bbox[shape_id][2], shapes_bbox[shape_id][3]]
                else:
                    if shapes_bbox[shape_id][0] < viajes_bbox[viaje["trip_id"]][0]:
                        viajes_bbox[viaje["trip_id"]][0] = shapes_bbox[shape_id][0]
                    elif shapes_bbox[shape_id][2] > viajes_bbox[viaje["trip_id"]][2]:
                        viajes_bbox[viaje["trip_id"]][2] = shapes_bbox[shape_id][2]
                    if shapes_bbox[shape_id][1] < viajes_bbox[viaje["trip_id"]][1]:
                        viajes_bbox[viaje["trip_id"]][1] = shapes_bbox[shape_id][1]
                    elif shapes_bbox[shape_id][3] > viajes_bbox[viaje["trip_id"]][3]:
                        viajes_bbox[viaje["trip_id"]][3] = shapes_bbox[shape_id][3]
    
    # Establecer BBOX de las lineas (por recorrido o paradas)
    lista_lineas = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "routes.txt"))
    
    lista_update = []
    for linea in lista_lineas:
        if linea["route_id"] in lineas_bbox.keys():
            lista_update.append(UpdateOne({"_id": linea["route_id"]}, {"$set": {"bbox": lineas_bbox[linea["route_id"]]}}))
        else:
            # Paradas de linea
            paradas = db_lineas.aggregate([
                {
                    '$match': {
                        '_id': linea["route_id"]
                    }
                }, 
                {
                    '$lookup': {
                        'from': 'paradas', 
                        'localField': 'paradas', 
                        'foreignField': '_id', 
                        'as': 'paradas'
                    }
                }, 
                {
                    '$project': {
                        'paradas': {
                            'posicionLatitud': 1, 
                            'posicionLongitud': 1
                        }
                    }
                }, 
                {
                    '$unwind': '$paradas'
                }, 
                {
                    '$replaceRoot': {
                        'newRoot': '$paradas'
                    }
                }
            ])

            bbox = [180, 90, -180, -90]
            for parada in paradas:
                if parada["posicionLongitud"] < bbox[0]:
                    bbox[0] = parada["posicionLongitud"]
                elif parada["posicionLongitud"] > bbox[2]:
                    bbox[2] = parada["posicionLongitud"]
                if parada["posicionLatitud"] < bbox[1]:
                    bbox[1] = parada["posicionLatitud"]
                elif parada["posicionLatitud"] > bbox[3]:
                    bbox[3] = parada["posicionLatitud"]
            
            lista_update.append(UpdateOne({"_id": linea["route_id"]}, {"$set": {"bbox": bbox}}))
    if len(lista_update) > 0:
        db_lineas.bulk_write(lista_update)


    # Establecer BBOX de los viajes
    lista_update = []
    for viaje in trip_list:
        if viaje["trip_id"] in viajes_bbox.keys():
            lista_update.append(UpdateOne({"_id": viaje["trip_id"]}, {"$set": {"bbox": viajes_bbox[viaje["trip_id"]]}}))
        else:
            # Paradas de viaje
            paradas = db_viajes.aggregate([
                {
                    '$match': {
                        '_id': viaje["trip_id"]
                    }
                }, 
                {
                    '$lookup': {
                        'from': 'paradas', 
                        'localField': 'paradas', 
                        'foreignField': '_id', 
                        'as': 'paradas'
                    }
                }, 
                {
                    '$project': {
                        'paradas': {
                            'posicionLatitud': 1, 
                            'posicionLongitud': 1
                        }
                    }
                }, 
                {
                    '$unwind': '$paradas'
                }, 
                {
                    '$replaceRoot': {
                        'newRoot': '$paradas'
                    }
                }
            ])

            bbox = [180, 90, -180, -90]
            for parada in paradas:
                if parada["posicionLongitud"] < bbox[0]:
                    bbox[0] = parada["posicionLongitud"]
                elif parada["posicionLongitud"] > bbox[2]:
                    bbox[2] = parada["posicionLongitud"]
                if parada["posicionLatitud"] < bbox[1]:
                    bbox[1] = parada["posicionLatitud"]
                elif parada["posicionLatitud"] > bbox[3]:
                    bbox[3] = parada["posicionLatitud"]
            
            lista_update.append(UpdateOne({"_id": viaje["trip_id"]}, {"$set": {"bbox": bbox}}))
    if len(lista_update) > 0:
        db_viajes.bulk_write(lista_update)
        

    # Guardar BBOX de agencias
    lista_update = []
    for agencia in agencias_bbox.keys():
        lista_update.append(UpdateOne({"_id": agencia}, {"$set": {"bbox": agencias_bbox[agencia]}}))
    if len(lista_update) > 0:
        db_agencias.bulk_write(lista_update)
    
    # Guardar bbox final
    geojson["bbox"] = bbox

    # write file
    with open(os.path.join(directorio_geojson, gtfs["idFeed"]+".geojson"), 'w') as outfile:
        json.dump(geojson, outfile)


def csv_to_list(archivo) -> List[dict]:
    lista = []
    with open(archivo, encoding="utf-8-sig") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            lista.append(fila)
    return lista


def csv_to_dict(archivo, primary_key: list) -> dict:
    diccionario = {}
    with open(archivo, encoding="utf-8-sig") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            diccionario["_".join([fila[p] for p in primary_key])] = fila
    return diccionario


def main():
    global config, directorio_gtfs, directorio_geojson
    start = datetime.now()
    with open('/server/config.json') as f:
        config = json.load(f)

    directorio_gtfs = os.path.join("/server", config["directorioGTFS"])
    directorio_geojson = os.path.join("/server", config["directorioGeoJson"])

    try:
        os.mkdir(directorio_geojson)
    except FileExistsError:
        pass

    cliente = conectar()
    db = cliente["gtfs"]

    feeds_eliminar_ids = db["feeds"].distinct("idFeed", {"$or": [{"actualizar.tiles": True}, {"eliminar": True}]})
    for feed_id in feeds_eliminar_ids:
        try:
            os.remove(os.path.join(directorio_geojson, feed_id+".geojson"))
        except FileNotFoundError:
            pass
    
    try:
        for feed in db["feeds"].find({"actualizar.tiles": True}):
            generar(feed, db["paradas"], db["lineas"], db["agencias"], db["viajes"])
            db["feeds"].update_many({"idFeed": feed["idFeed"]}, {"$set": {"actualizar.tiles": False}})
            sys.stdout.flush()
    finally:
        print(f"Acabado en {(datetime.now()-start).total_seconds()}s")
        sys.stdout.flush()


if __name__ == '__main__':
    main()