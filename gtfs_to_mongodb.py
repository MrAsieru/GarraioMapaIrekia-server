import os
import json
import csv
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.typings import _DocumentType
from datetime import datetime, date, timedelta, time

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
    # Agencia
    lista_agencias = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "agency.txt"), ["agency_id"])
    colleccion_agencias = db["agencias"]
    
    for agencia_key in lista_agencias.keys():
        agencia: dict = lista_agencias[agencia_key]
        doc = {
            "idAgencia": agencia.get("agency_id"),
            "nombre": agencia.get("agency_name"),
            "url": agencia.get("agency_url"),
            "zonaHoraria": agencia.get("agency_timezone"),
            "idioma": agencia.get("agency_lang"),
            "telefono": agencia.get("agency_phone"),
            "urlTarifa": agencia.get("agency_fare_url"),
            "email": agencia.get("agency_email")
        }
        doc_id = colleccion_agencias.insert_one(doc).inserted_id
        agencia["mongodb_id"] = doc_id

    # Linea
    lista_lineas = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "routes.txt"), ["route_id"])
    colleccion_lineas = db["lineas"]

    for route_key in lista_lineas.keys():
        linea: dict = lista_lineas[route_key]
        doc = {
            "idLinea": linea.get("route_id"),
            "idAgencia": linea.get("agency_id"),
            "nombreCorto": linea.get("route_short_name"),
            "nombreLargo": linea.get("route_long_name"),
            "descripcion": linea.get("route_desc"),
            "tipo": linea.get("route_type"),
            "url": linea.get("route_url"),
            "color": linea.get("route_color") if linea.get("route_color", "").startswith('#') else "#"+linea.get("route_color", ""),
            "colorTexto": linea.get("route_text_color") if linea.get("route_text_color", "").startswith('#') else "#"+linea.get("route_text_color", ""),
            "orden": linea.get("route_sort_order"),
            "recogidaContinua": linea.get("continuous_pickup"),
            "bajadaContinua": linea.get("continuous_drop_off"),
            "idRed": linea.get("network_id"),
            "agencia": lista_agencias[linea["agency_id"]].get("mongodb_id")
        }
        doc_id = colleccion_lineas.insert_one(doc).inserted_id
        linea["mongodb_id"] = doc_id

    ## Agencia.lineas
    for agencia_key in lista_agencias.keys():
        agencia: dict = lista_agencias[agencia_key]
        lineas = []
        for linea_key in lista_lineas.keys():
            linea: dict = lista_lineas[linea_key]
            if linea["agency_id"] == agencia["agency_id"]:
                lineas.append(linea["mongodb_id"])
        colleccion_agencias.update_one({"_id": agencia["mongodb_id"]}, {"$set": {"lineas": lineas}})

    # Parada
    lista_paradas = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "stops.txt"), ["stop_id"])
    colleccion_paradas = db["paradas"]
    for parada_key in lista_paradas.keys():
        parada: dict = lista_paradas[parada_key]
        doc = {
            "idParada": parada.get("stop_id"),
            "codigo": parada.get("stop_code"),
            "nombre": parada.get("stop_name"),
            "descripcion": parada.get("stop_desc"),
            "posicionLatitud": parada.get("stop_lat"),
            "posicionLongitud": parada.get("stop_lon"),
            "idZona": parada.get("zone_id"),
            "url": parada.get("stop_url"),
            "tipo": parada.get("location_type") if parada.get("location_type", "") != "" else "0",
            "idParadaPadre": parada.get("parent_station"),
            "zonaHoraria": parada.get("stop_timezone"),
            "accesibilidad": parada.get("wheelchair_boarding"),
            "idNivel": parada.get("level_id"),
            "codigoPlataforma": parada.get("platform_code")
        }
        doc_id = colleccion_paradas.insert_one(doc).inserted_id
        parada["mongodb_id"] = doc_id

    ## Parada.paradaPadre
    for parada_key in lista_paradas.keys():
        parada: dict = lista_paradas[parada_key]
        
        if parada.get("parent_station") is not None and parada.get("parent_station") != "":
            paradaPadre = lista_paradas.get(parada["parent_station"])
            colleccion_paradas.update_one({"_id": parada["mongodb_id"]}, {"$set": {"paradaPadre": paradaPadre["mongodb_id"]}})

    # Viaje
    lista_viajes = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "trips.txt"), ["trip_id"])
    colleccion_viajes = db["viajes"]

    for viaje_key in lista_viajes.keys():
        viaje: dict = lista_viajes[viaje_key]
        viaje["linea"] = lista_lineas[viaje["route_id"]].get("mongodb_id")
        doc = {
            "idViaje": viaje.get("trip_id"),
            "idLinea": viaje.get("route_id"),
            "idServicio": viaje.get("service_id"),
            "letrero": viaje.get("trip_headsign"),
            "nombre": viaje.get("trip_short_name"),
            "idDireccion": viaje.get("direction_id"),
            "idBloque": viaje.get("block_id"),
            "idRecorrido": viaje.get("shape_id"),
            "accesibilidad": viaje.get("wheelchair_accessible"),
            "bicicletas": viaje.get("bikes_allowed"),
            "linea": viaje["linea"]
        }
        doc_id = colleccion_viajes.insert_one(doc).inserted_id
        viaje["mongodb_id"] = doc_id

    ## Linea.viajes
    for linea_key in lista_lineas.keys():
        linea: dict = lista_lineas[linea_key]
        colleccion_lineas.update_one({"_id": linea.get("mongodb_id")}, {"$set": {"viajes": [lista_viajes[v]["mongodb_id"] for v in lista_viajes.keys() if lista_viajes[v].get("route_id") == linea["route_id"]]}})

    ## Viaje.horarios y Viaje.paradas
    lista_horarios = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "stop_times.txt"), ["trip_id"])

    for viaje_key in lista_viajes.keys():
        viaje: dict = lista_viajes[viaje_key]
        
        horarios = []
        for servicio in lista_horarios.get(viaje["trip_id"]):
            servicio["parada_mongodb_id"] = lista_paradas[servicio["stop_id"]].get("mongodb_id")
            horarios.append({
                "parada": servicio["parada_mongodb_id"],
                "idParada": servicio.get("stop_id"),
                "horaLlegada": servicio.get("arrival_time"),
                "horaSalida": servicio.get("departure_time"),
                "orden": servicio.get("stop_sequence"),
                "letrero": servicio.get("stop_headsign"),
                "tipoRecogida": servicio.get("pickup_type"),
                "tipoBajada": servicio.get("drop_off_type"),
                "recogidaContinua": servicio.get("continuous_pickup"),
                "bajadaContinua": servicio.get("continuous_drop_off"),
                "distanciaRecorrida": servicio.get("shape_dist_traveled"),
                "exacto": servicio.get("timepoint")
            })

        viaje["paradas"] = [h["parada"] for h in horarios]

        colleccion_viajes.update_one({"_id": viaje.get("mongodb_id")}, {"$set": {"horarios": horarios}})
        colleccion_viajes.update_one({"_id": viaje.get("mongodb_id")}, {"$set": {"paradas": viaje["paradas"]}})

    ## Linea.paradas
    for linea_key in lista_lineas.keys():
        linea: dict = lista_lineas[linea_key]
        paradas = []
        for viaje_key in lista_viajes.keys():
            viaje: dict = lista_viajes[viaje_key]
            if viaje["route_id"] == linea["route_id"]:
                paradas.extend(viaje["paradas"])

        paradas = list(set(paradas))
        colleccion_lineas.update_one({"_id": linea.get("mongodb_id")}, {"$set": {"paradas": paradas}})

    ## Parada.lineas y Parada.viajes
    for parada_key in lista_paradas.keys():
        parada: dict = lista_paradas[parada_key]
        lineas = []
        viajes = []
        for viaje_key in lista_viajes.keys():
            viaje: dict = lista_viajes[viaje_key]
            if parada["mongodb_id"] in viaje["paradas"]:
                lineas.append(viaje["linea"])
                viajes.append(viaje["mongodb_id"])

        lineas = list(set(lineas))

        colleccion_paradas.update_one({"_id": parada.get("mongodb_id")}, {"$set": {"lineas": lineas}})
        colleccion_paradas.update_one({"_id": parada.get("mongodb_id")}, {"$set": {"viajes": viajes}})

    ## Area
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "areas.txt")) and os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "area_stops.txt"))):
        lista_areas = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "areas.txt"))
        lista_paradas_area = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "area_stops.txt"), ["area_id"])
        colleccion_areas = db["areas"]

        for area in lista_areas:
            paradas = []
            for servicio in lista_paradas_area.get(area["area_id"]):
                paradas.append(lista_paradas[servicio["stop_id"]].get("mongodb_id"))

            doc = {
                "idArea": area.get("area_id"),
                "nombre": area.get("area_name"),
                "paradas": paradas
            }
            doc_id = colleccion_areas.insert_one(doc).inserted_id
            area["mongodb_id"] = doc_id

    ## Parada.areas
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "areas.txt")) and os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "area_stops.txt"))):
        for parada_key in lista_paradas.keys():
            parada: dict = lista_paradas[parada_key]
            areas = []
            for area in lista_areas:
                if parada["mongodb_id"] in area["paradas"]:
                    areas.append(area["mongodb_id"])

            colleccion_paradas.update_one({"_id": parada.get("mongodb_id")}, {"$set": {"areas": areas}})

    ## Parada.nivel
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "levels.txt"))):
        lista_niveles = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "levels.txt"), ["level_id"])
        for parada_key in lista_paradas.keys():
            parada: dict = lista_paradas[parada_key]
            if parada.get("level_id") is not None:
                nivel: dict = lista_niveles[parada["level_id"]]
                doc = {
                    "idNivel": nivel.get("level_id"),
                    "indice": nivel.get("level_index"),
                    "nombre": nivel.get("level_name")
                }
                colleccion_paradas.update_one({"_id": parada.get("mongodb_id")}, {"$set": doc})

    ## Itinerario
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "pathways.txt"))):
        lista_itinerarios = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "pathways.txt"), ["shape_id"])
        colleccion_itinerarios = db["itinerarios"]

        for itinerario_key in lista_itinerarios.keys():
            itinerario: dict = lista_itinerarios[itinerario_key]
            doc = {
                "idItinerario": itinerario.get("pathway_id"),
                "desdeParada": lista_paradas[itinerario["from_stop_id"]].get("mongodb_id"),
                "hastaParada": lista_paradas[itinerario["to_stop_id"]].get("mongodb_id"),
                "modo": itinerario.get("pathway_mode"),
                "bidireccional": itinerario.get("is_bidirectional"),
                "distancia": itinerario.get("length"),
                "duracion": itinerario.get("traversal_time"),
                "escalones": itinerario.get("stair_count"),
                "pendienteMax": itinerario.get("max_slope"),
                "anchuraMin": itinerario.get("min_width"),
                "letrero": itinerario.get("signposted_as"),
                "letreroReverso": itinerario.get("reversed_signposted_as")
            }

            doc_id = colleccion_itinerarios.insert_one(doc).inserted_id
            itinerario["mongodb_id"] = doc_id

    # Recorrido
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt"))):
        lista_recorridos = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt"), ["shape_id"])
        colleccion_recorridos = db["recorridos"]

        for recorrido_key in lista_recorridos.keys():
            recorrido: dict = lista_recorridos[recorrido_key]
            
            secuencia = []
            for servicio in recorrido:
                secuencia.append({
                    "latitud": servicio.get("shape_pt_lat"),
                    "longitud": servicio.get("shape_pt_lon"),
                    "orden": servicio.get("shape_pt_sequence"),
                    "distancia": servicio.get("shape_dist_traveled")
                })
            
            doc = {
                "idRecorrido": recorrido_key,
                "secuencia": secuencia
            }

            colleccion_recorridos.insert_one(doc).inserted_id

    # Calendario
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar.txt"))): 
        lista_calendario = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar.txt"))
    else:
        lista_calendario = []
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar_dates.txt"))):
        lista_fechas_calendario = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar_dates.txt"))
    else:
        lista_fechas_calendario = []
    colleccion_calendario = db["calendario"]

    lista_servicios = {}
    
    for servicio in lista_calendario:
        fecha_inicio = datetime.strptime(servicio["start_date"], "%Y%m%d").date()
        fecha_fin = datetime.strptime(servicio["end_date"], "%Y%m%d").date()
        fecha_actual = datetime.now().date()

        # Marcar como fecha inicio el dia actual para evitar generar demasiadas fechas        
        if fecha_inicio < fecha_actual:
            if fecha_actual <= fecha_fin:
                fecha_inicio = fecha_actual
            else:
                continue

        # Obtener dias de la semana en los que hay servicio (ISO 8601)
        dias_servicio = []
        dias_semana = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        for i in range(len(dias_semana)):
            if servicio[dias_semana[i]] == "1":
                dias_servicio.append(i+1)

        # Obtener todas las fechas en las que existe el servicio
        fechas_servicio = []
        i = fecha_inicio
        while i <= fecha_fin:
            if i.isoweekday() in dias_servicio:
                fechas_servicio.append(datetime.combine(i, time.min))
            i += timedelta(days=1)
        
        lista_servicios[servicio["service_id"]] = fechas_servicio

    for servicio in lista_fechas_calendario:
        if not servicio["service_id"] in lista_servicios.keys():
            lista_servicios[servicio["service_id"]] = []
        
        fecha = datetime.strptime(servicio["date"], "%Y%m%d")
        if servicio["exception_type"] == "1":
            lista_servicios[servicio["service_id"]].append(fecha)
        elif servicio["exception_type"] == "2":
            lista_servicios[servicio["service_id"]].remove(fecha)

    # Eliminar posibles fechas repetidas
    for servicio in lista_servicios.keys():
        lista_servicios[servicio] = list(set(lista_servicios[servicio]))
        lista_servicios[servicio].sort()

    # Obtener todas las fechas
    fechas_servicios = []
    for servicio in lista_servicios.keys():
        fechas_servicios.extend(lista_servicios[servicio])

    fechas = list(set(fechas_servicios))

    # Guardar fechas y sus servicios
    for fecha in fechas:
        servicios = []
        for servicio in lista_servicios.keys():
            if fecha in lista_servicios[servicio]:
                servicios.append(servicio)

        doc = {
            "fecha": fecha,
            "servicios": servicios
        }
        colleccion_calendario.insert_one(doc)

    ## Viaje.fechas
    for viaje_key in lista_viajes.keys():
        viaje: dict = lista_viajes[viaje_key]

        colleccion_viajes.update_one({"_id": viaje.get("mongodb_id")}, {"$set": {"fechas": lista_servicios.get(viaje["service_id"])}})
        
    ## Viaje.frecuencias
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "frequencies.txt"))):
        lista_frecuencias = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "frequencies.txt"), ["trip_id"])

        for viaje_key in lista_viajes.keys():
            viaje: dict = lista_viajes[viaje_key]

            frecuencias = []
            for item in lista_frecuencias.get(viaje["trip_id"]) or []:
                frecuencias.append({
                    "horaInicio": item.get("start_time"),
                    "horaFin": item.get("end_time"),
                    "margen": item.get("headway_secs"),
                    "exacto": item.get("exact_times") == "1"
                })

            colleccion_viajes.update_one({"_id": viaje.get("mongodb_id")}, {"$set": {"frecuencias": frecuencias}})

    ## Feed.info
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "feed_info.txt"))):
        lista_info = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "feed_info.txt"))
        colleccion_feed = db["feeds"]

        if len(lista_info) > 0:
            info = lista_info[0]
            doc = {
                "nombreEditor": info.get("feed_publisher_name"),
                "urlEditor": info.get("feed_publisher_url"),
                "idioma": info.get("feed_lang"),
                "idiomaPredeterminado": info.get("default_lang"),
                "fechaInicio": info.get("feed_start_date"),
                "fechaFin": info.get("feed_end_date"),
                "version": info.get("feed_version"),
                "email": info.get("feed_contact_email"),
                "urlContacto": info.get("feed_contact_url")
            }
            colleccion_feed.update_one({"idFeed": gtfs["idFeed"]}, {"$set": {"info": doc}})

    ## Feed.atribuciones
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "attributions.txt"))):
        lista_atribuciones = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "attributions.txt"))

        atribuciones = []
        for atribucion in lista_atribuciones:
            atribuciones.append({
                "idAtribucion": atribucion.get("attribution_id"),
                "idAgencia": atribucion.get("agency_id"),
                "idLinea": atribucion.get("route_id"),
                "idViaje": atribucion.get("trip_id"),
                "nombreOrganizacion": atribucion.get("agency_name"),
                "esProductor": atribucion.get("is_producer") == "1",
                "esOperador": atribucion.get("is_operator") == "1",
                "esAutoridad": atribucion.get("is_authority") == "1",
                "url": atribucion.get("agency_url"),
                "email": atribucion.get("agency_email"),
                "telefono": atribucion.get("agency_phone")
            })

        colleccion_feed.update_one({"idFeed": gtfs["idFeed"]}, {"$set": {"atribuciones": doc}})

    ## Traduccion
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "translations.txt"))):
        lista_traducciones = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "translations.txt"))
        colleccion_traducciones = db["traducciones"]

        for traduccion in lista_traducciones:
            doc = {
                "nombreTabla": traduccion.get("table_name"),
                "nombreCampo": traduccion.get("field_name"),
                "idioma": traduccion.get("language"),
                "traduccion": traduccion.get("translation"),
                "idElemento": traduccion.get("record_id"),
                "idElemento2": traduccion.get("record_sub_id"),
                "valorOriginal": traduccion.get("field_value")
            }
            colleccion_traducciones.insert_one(doc)

        db["feeds"].update_one({"id": gtfs["idFeed"]}, {"$set": {"actualizar": False}})


def csv_to_dict(archivo, primary_key: list) -> dict:
    diccionario = {}
    with open(archivo, encoding="utf-8-sig") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            diccionario["_".join([fila[p] for p in primary_key])] = fila
    return diccionario


def csv_to_listdict(archivo, primary_key: list) -> dict:
    lista = {}
    with open(archivo, encoding="utf-8-sig") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            pk = "_".join([fila[p] for p in primary_key])
            if not pk in lista.keys():
                lista[pk] = []
            lista[pk].append(fila)
    return lista


def csv_to_list(archivo) -> list:
    lista = []
    with open(archivo, encoding="utf-8-sig") as datos_csv:
        reader = csv.DictReader(datos_csv)
        for fila in reader:
            lista.append(fila)
    return lista

def main():
    global config, directorio_gtfs, directorio_geojson
    start = datetime.now()
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

    #TODO: Eliminar documentos que contengan id con el prefijo de los feeds que se deben actualizar
    # feeds_actualizar_ids = db["feeds"].distinct("id", {"actualizar": True})
    # for colleccion in ["agencies", "routes", "stops"]:
    #     db[colleccion].delete_many({"idFeed": {"$regex": f"^{('|'.join(feeds_actualizar_ids))}_"}})
    try:
        for feed in db["feeds"].find({"actualizar": True}):
            guardar(feed, db)
    finally:

    
        print(f"Acabado en {(datetime.now()-start).total_seconds()}s")


if __name__ == '__main__':
    main()