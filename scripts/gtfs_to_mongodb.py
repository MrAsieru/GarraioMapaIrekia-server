import os
import csv
from pymongo import UpdateOne
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.database import Database
from pymongo.typings import _DocumentType
from datetime import datetime, timedelta, time

directorio_gtfs = "/server/gtfs"

def conectar():
    uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@mongodb:27017/gtfs"
    
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente


def none_si_vacio(valor):
    if valor == "": return None
    return valor

def guardar(gtfs, db: Database[_DocumentType]):
    global directorio_gtfs
    #region Agencia
    lista_agencias = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "agency.txt"), ["agency_id"])
    colleccion_agencias = db["agencias"]
    
    lista_documentos = []
    for agencia_key in lista_agencias.keys():
        agencia: dict = lista_agencias[agencia_key]
        lista_documentos.append({
            "_id": agencia.get("agency_id"),
            "idAgencia": agencia.get("agency_id"),
            "nombre": agencia.get("agency_name"),
            "url": agencia.get("agency_url"),
            "zonaHoraria": agencia.get("agency_timezone"),
            "idioma": none_si_vacio(agencia.get("agency_lang", "")),
            "telefono": none_si_vacio(agencia.get("agency_phone", "")),
            "urlTarifa": none_si_vacio(agencia.get("agency_fare_url", "")),
            "email": none_si_vacio(agencia.get("agency_email", ""))
        })
    colleccion_agencias.insert_many(lista_documentos)
    #endregion

    #region Linea
    lista_lineas = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "routes.txt"), ["route_id"])
    colleccion_lineas = db["lineas"]

    lista_documentos = []
    agencia_lineas = {}
    for route_key in lista_lineas.keys():
        linea: dict = lista_lineas[route_key]
        color = linea.get("route_color", None)
        if color is not None and color != "":
            color = "#"+color
        colorTexto = linea.get("route_text_color", None)
        if colorTexto is not None and colorTexto != "":
            colorTexto = "#"+colorTexto
        lista_documentos.append({
            "_id": linea.get("route_id"),
            "idLinea": linea.get("route_id"),
            "idAgencia": linea.get("agency_id"),
            "nombreCorto": none_si_vacio(linea.get("route_short_name", "")),
            "nombreLargo": none_si_vacio(linea.get("route_long_name", "")),
            "descripcion": none_si_vacio(linea.get("route_desc", "")),
            "tipo": int(linea.get("route_type")),
            "url": none_si_vacio(linea.get("route_url", "")),
            "color": none_si_vacio(color),
            "colorTexto": none_si_vacio(colorTexto),
            "orden": int(linea.get("route_sort_order")) if linea.get("route_sort_order", "") != "" else None,
            "recogidaContinua": int(linea.get("continuous_pickup") if linea.get("continuous_pickup", "") != "" else "1"),
            "bajadaContinua": int(linea.get("continuous_drop_off") if linea.get("continuous_drop_off", "") != "" else "1"),
            "idRed": none_si_vacio(linea.get("network_id", ""))
        })

        # Agencia.lineas
        if not linea["agency_id"] in agencia_lineas.keys():
            agencia_lineas[linea["agency_id"]] = []
        agencia_lineas[linea["agency_id"]].append(linea["route_id"])

    colleccion_lineas.insert_many(lista_documentos)

    # Agencia.lineas
    lista_updates = []
    for agencia_key in agencia_lineas.keys():
        lista_updates.append(UpdateOne({"_id": agencia_key}, {"$set": {"lineas": agencia_lineas[agencia_key]}}))
    if (len(lista_updates) > 0):
        colleccion_agencias.bulk_write(lista_updates)
    #endregion

    #region Parada
    lista_paradas = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "stops.txt"), ["stop_id"])
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "levels.txt"))):
        lista_niveles = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "levels.txt"), ["level_id"])
    colleccion_paradas = db["paradas"]

    paradas_padre = {} # paradaPadre: [paradasHijas]

    lista_documentos = []
    for parada_key in lista_paradas.keys():
        parada: dict = lista_paradas[parada_key]

        # Añadir parada a la lista de paradas hijas de su parada padre
        if (parada.get("parent_station", "") != ""):
            if not parada["parent_station"] in paradas_padre.keys():
                paradas_padre[parada["parent_station"]] = []
            paradas_padre[parada["parent_station"]].append(parada["stop_id"])

        doc = {
            "_id": parada.get("stop_id"),
            "idParada": parada.get("stop_id"),
            "codigo": none_si_vacio(parada.get("stop_code", "")),
            "nombre": none_si_vacio(parada.get("stop_name", "")),
            "nombreTts": none_si_vacio(parada.get("tts_stop_name", "")),
            "descripcion": none_si_vacio(parada.get("stop_desc", "")),
            "posicionLatitud": round(float(parada.get("stop_lat")), 5) if parada.get("stop_lat", "") != "" else None,
            "posicionLongitud": round(float(parada.get("stop_lon")), 5) if parada.get("stop_lon", "") != "" else None,
            "idZona": none_si_vacio(parada.get("zone_id", "")),
            "url": none_si_vacio(parada.get("stop_url", "")),
            "tipo": int(parada.get("location_type") if parada.get("location_type", "") != "" else "0"),
            "paradaPadre": none_si_vacio(parada.get("parent_station", "")),
            "zonaHoraria": none_si_vacio(parada.get("stop_timezone", "")),
            "accesibilidad": int(parada.get("wheelchair_boarding") if parada.get("wheelchair_boarding", "") != "" else "0"),
            "idNivel": none_si_vacio(parada.get("level_id", "")),
            "codigoPlataforma": none_si_vacio(parada.get("platform_code", ""))
        }

        # Parada.nivel
        if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "levels.txt"))) and parada.get("level_id") is not None:
            nivel: dict = lista_niveles[parada["level_id"]]
            doc["nivel"] = {
                "idNivel": nivel.get("level_id"),
                "indice": float(nivel.get("level_index")),
                "nombre": none_si_vacio(nivel.get("level_name", ""))
            }

        lista_documentos.append(doc)

    colleccion_paradas.insert_many(lista_documentos)
    #endregion

    # #region Parada.paradasHijas
    # lista_updates = []
    # for parada_key in paradas_padre.keys():
    #     lista_updates.append(UpdateOne({"_id": parada_key}, {"$set": {"paradasHijas": paradas_padre[parada_key]}}))
    # if len(lista_updates) > 0:
    #     colleccion_paradas.bulk_write(lista_updates)
    # #endregion

    #region Viaje
    lista_viajes = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "trips.txt"), ["trip_id"])
    lista_horarios = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "stop_times.txt"), ["trip_id"])
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "frequencies.txt"))):
        lista_frecuencias = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "frequencies.txt"), ["trip_id"])
    colleccion_viajes = db["viajes"]

    lista_documentos = []
    linea_viajes = {}
    linea_paradas = {}
    parada_lineas = {}
    parada_viajes = {}
    for viaje_key in lista_viajes.keys():
        viaje: dict = lista_viajes[viaje_key]

        horarios = []
        for item in lista_horarios.get(viaje["trip_id"]) or []:
            horarios.append({
                "idParada": item.get("stop_id"),
                "horaLlegada": none_si_vacio(item.get("arrival_time", "")),
                "horaSalida": none_si_vacio(item.get("departure_time", "")),
                "orden": int(item.get("stop_sequence")),
                "letrero": none_si_vacio(item.get("stop_headsign", "")),
                "tipoRecogida": int(item.get("pickup_type") if item.get("pickup_type", "") != "" else "0"),
                "tipoBajada": int(item.get("drop_off_type") if item.get("drop_off_type", "") != "" else "0"),
                "recogidaContinua": int(item.get("continuous_pickup") if item.get("continuous_pickup", "") != "" else "1"),
                "bajadaContinua": int(item.get("continuous_drop_off") if item.get("continuous_drop_off", "") != "" else "1"),
                "distanciaRecorrida": float(item.get("shape_dist_traveled")) if item.get("shape_dist_traveled", "") != "" else None,
                "exacto": item.get("timepoint", "1") == "1" or item.get("timepoint") == ""
            })

            # Parada.lineas
            if not item["stop_id"] in parada_lineas.keys():
                parada_lineas[item["stop_id"]] = []
            parada_lineas[item["stop_id"]].append(viaje["route_id"])

            # Parada.viajes
            if not item["stop_id"] in parada_viajes.keys():
                parada_viajes[item["stop_id"]] = []
            parada_viajes[item["stop_id"]].append(viaje["trip_id"])

        doc = {
            "_id": viaje.get("trip_id"),
            "idAgencia": lista_lineas.get(viaje.get("route_id"), {}).get("agency_id"),
            "idViaje": viaje.get("trip_id"),
            "idLinea": viaje.get("route_id"),
            "idServicio": viaje.get("service_id"),
            "letrero": none_si_vacio(viaje.get("trip_headsign", "")),
            "nombre": none_si_vacio(viaje.get("trip_short_name", "")),
            "direccion": int(viaje.get("direction_id")) if viaje.get("direction_id", "") != "" else None,
            "idBloque": none_si_vacio(viaje.get("block_id", "")),
            "idRecorrido": none_si_vacio(viaje.get("shape_id", "")),
            "accesibilidad": int(viaje.get("wheelchair_accessible") if viaje.get("wheelchair_accessible", "") != "" else "0"),
            "bicicletas": int(viaje.get("bikes_allowed") if viaje.get("bikes_allowed", "") != "" else "0"),
            "horarios": horarios,
            "paradas": [h["idParada"] for h in horarios]
        }

        # Viaje.frecuencias
        if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "frequencies.txt"))):
            frecuencias = []
            for item in lista_frecuencias.get(viaje["trip_id"]) or []:
                frecuencias.append({
                    "horaInicio": item.get("start_time"),
                    "horaFin": item.get("end_time"),
                    "margen": int(item.get("headway_secs")),
                    "exacto": item.get("exact_times", "0") == "1" or item.get("exact_times") != ""
                })
            doc["frecuencias"] = frecuencias

        lista_documentos.append(doc)

        # Linea.viajes
        if not viaje["route_id"] in linea_viajes.keys():
            linea_viajes[viaje["route_id"]] = []
        linea_viajes[viaje["route_id"]].append(viaje["trip_id"])

        # Linea.paradas
        if not viaje["route_id"] in linea_paradas.keys():
            linea_paradas[viaje["route_id"]] = []
        linea_paradas[viaje["route_id"]].extend([h["idParada"] for h in horarios])

    colleccion_viajes.insert_many(lista_documentos)

    # Linea.viajes
    lista_updates = []
    for linea_key in linea_viajes.keys():
        lista_updates.append(UpdateOne({"_id": linea_key}, {"$set": {"viajes": linea_viajes[linea_key]}}))
    if (len(lista_updates) > 0):
        colleccion_lineas.bulk_write(lista_updates)

    # Linea.patrones
    lista_updates = []
    for linea_key in linea_viajes.keys():
        patrones_linea_tuplas = {} # Patron: {de: idParada, a: idParada, direccion: number}
        for viaje in linea_viajes[linea_key]:
            horario = sorted(lista_horarios.get(viaje) or [], key=lambda x: int(x["stop_sequence"]))
            secuencia_paradas = tuple([item["stop_id"] for item in horario])
            if (len(secuencia_paradas) > 0):
                tmp = patrones_linea_tuplas.get(secuencia_paradas, None)
                if tmp is None:
                    patrones_linea_tuplas[secuencia_paradas] = {
                        "de": lista_paradas[secuencia_paradas[0]].get("stop_name", ""),
                        "a": lista_paradas[secuencia_paradas[-1]].get("stop_name", ""),
                        "direccion": none_si_vacio(lista_viajes[viaje].get("direction_id", "")),
                        "letrero": none_si_vacio(lista_viajes[viaje].get("trip_headsign", "")),
                        "numViajes": 1
                    }
                else:
                    tmp["numViajes"] += 1
        patrones = []
        for patron in patrones_linea_tuplas.keys():
            patrones.append({
                "de": patrones_linea_tuplas[patron]["de"],
                "a": patrones_linea_tuplas[patron]["a"],
                "direccion": patrones_linea_tuplas[patron]["direccion"],
                "letrero": patrones_linea_tuplas[patron]["letrero"],
                "numViajes": patrones_linea_tuplas[patron]["numViajes"],
                "paradas": list(patron)
            })
        lista_updates.append(UpdateOne({"_id": linea_key}, {"$set": {"patrones": patrones}}))
    if (len(lista_updates) > 0):
        colleccion_lineas.bulk_write(lista_updates)


    # Linea.paradas
    lista_updates = []
    for linea_key in linea_paradas.keys():
        lista_updates.append(UpdateOne({"_id": linea_key}, {"$set": {"paradas": list(set(linea_paradas[linea_key]))}}))
    if (len(lista_updates) > 0):
        colleccion_lineas.bulk_write(lista_updates)

    # Parada.lineas
    lista_updates = []
    for parada_key in parada_lineas.keys():
        lista_updates.append(UpdateOne({"_id": parada_key}, {"$set": {"lineas": list(set(parada_lineas[parada_key]))}}))
    if (len(lista_updates) > 0):
        colleccion_paradas.bulk_write(lista_updates)
    
    # Parada.agencias
    lista_updates = []
    for parada_key in parada_lineas.keys():
        # lista de agency_id de las lineas de una parada
        lista_updates.append(UpdateOne({"_id": parada_key}, {"$set": {"agencias": list(set([lista_lineas[linea]["agency_id"] for linea in set(parada_lineas[parada_key])]))}}))
    if (len(lista_updates) > 0):
        colleccion_paradas.bulk_write(lista_updates)

    # Parada.viajes
    lista_updates = []
    for parada_key in parada_viajes.keys():
        lista_updates.append(UpdateOne({"_id": parada_key}, {"$set": {"viajes": parada_viajes[parada_key]}}))
    if (len(lista_updates) > 0):
        colleccion_paradas.bulk_write(lista_updates)
    #endregion

    # #region Area
    # if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "areas.txt")) and os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "area_stops.txt"))):
    #     lista_areas = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "areas.txt"))
    #     lista_paradas_area = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "area_stops.txt"), ["area_id"])
    #     colleccion_areas = db["areas"]

    #     lista_documentos = []
    #     parada_areas = {}
    #     for area in lista_areas:
    #         paradas = []
    #         for item in lista_paradas_area.get(area["area_id"]):
    #             paradas.append(item["stop_id"])

    #             # Parada.areas
    #             if not item["stop_id"] in parada_areas.keys():
    #                 parada_areas[item["stop_id"]] = []
    #             parada_areas[item["stop_id"]].append(area["area_id"])

    #         lista_documentos.append({
    #             "_id": area.get("area_id"),
    #             "idArea": area.get("area_id"),
    #             "nombre": none_si_vacio(area.get("area_name", "")),
    #             "paradas": paradas
    #         })

    #     colleccion_areas.insert_many(lista_documentos)

    #     # Parada.areas
    #     lista_updates = []
    #     for parada_key in parada_areas.keys():
    #         lista_updates.append(UpdateOne({"_id": parada_key}, {"$set": {"areas": parada_areas[parada_key]}}))
    #     if (len(lista_updates) > 0):
    #         colleccion_paradas.bulk_write(lista_updates)
    # #endregion

    # #region Itinerario
    # if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "pathways.txt"))):
    #     lista_itinerarios = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "pathways.txt"), ["shape_id"])
    #     colleccion_itinerarios = db["itinerarios"]

    #     lista_documentos = []
    #     for itinerario_key in lista_itinerarios.keys():
    #         itinerario: dict = lista_itinerarios[itinerario_key]
    #         lista_documentos.append({
    #             "_id": itinerario.get("pathway_id"),
    #             "idItinerario": itinerario.get("pathway_id"),
    #             "desdeParada": itinerario["from_stop_id"],
    #             "hastaParada": itinerario["to_stop_id"],
    #             "modo": int(itinerario.get("pathway_mode")),
    #             "bidireccional": itinerario.get("is_bidirectional") == "1",
    #             "distancia": float(itinerario.get("length")) if itinerario.get("length", "") != "" else None,
    #             "duracion": int(itinerario.get("traversal_time")) if itinerario.get("traversal_time", "") != "" else None,
    #             "escalones": int(itinerario.get("stair_count")) if itinerario.get("stair_count", "") != "" else None,
    #             "pendienteMax": float(itinerario.get("max_slope") if itinerario.get("max_slope", "") != "" else "0"),
    #             "anchuraMin": float(itinerario.get("min_width")) if itinerario.get("min_width", "") != "" else None,
    #             "letrero": none_si_vacio(itinerario.get("signposted_as", "")),
    #             "letreroReverso": none_si_vacio(itinerario.get("reversed_signposted_as", ""))
    #         })

    #     colleccion_itinerarios.insert_many(lista_documentos)
    # #endregion

    #region Recorrido
    # if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt"))):
    #     lista_recorridos = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt"), ["shape_id"])
    #     colleccion_recorridos = db["recorridos"]

    #     lista_documentos = []
    #     for recorrido_key in lista_recorridos.keys():
    #         recorrido: dict = lista_recorridos[recorrido_key]
            
    #         secuencia = []
    #         for item in recorrido:
    #             secuencia.append({
    #                 "latitud": round(float(item.get("shape_pt_lat")), 5),
    #                 "longitud": round(float(item.get("shape_pt_lon")), 5),
    #                 "orden": int(item.get("shape_pt_sequence")),
    #                 "distancia": float(item.get("shape_dist_traveled")) if item.get("shape_dist_traveled", "") != "" else None
    #             })
            
    #         lista_documentos.append({
    #             "_id": recorrido_key,
    #             "idRecorrido": recorrido_key,
    #             "secuencia": secuencia
    #         })

    #     colleccion_recorridos.insert_many(lista_documentos)
    #endregion

    #region Calendario
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
    
    for item in lista_calendario:
        fecha_inicio = datetime.strptime(item["start_date"], "%Y%m%d")
        fecha_fin = datetime.strptime(item["end_date"], "%Y%m%d")
        fecha_actual = datetime.combine(datetime.now().date(), time.min)

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
            if item[dias_semana[i]] == "1":
                dias_servicio.append(i+1)

        # Obtener todas las fechas en las que existe el servicio
        fechas_servicio = []
        i = fecha_inicio
        while i <= fecha_fin:
            if i.isoweekday() in dias_servicio:
                fechas_servicio.append(i)
            i += timedelta(days=1)
        
        lista_servicios[item["service_id"]] = fechas_servicio

    for item in lista_fechas_calendario:
        if not item["service_id"] in lista_servicios.keys():
            lista_servicios[item["service_id"]] = []
        
        fecha = datetime.strptime(item["date"], "%Y%m%d")
        if fecha_actual <= fecha:
            if item["exception_type"] == "1":
                lista_servicios[item["service_id"]].append(fecha)
            elif item["exception_type"] == "2":
                try:
                    lista_servicios[item["service_id"]].remove(fecha)
                except ValueError:
                    pass

    # Eliminar posibles fechas repetidas
    for item in lista_servicios.keys():
        lista_servicios[item] = list(set(lista_servicios[item]))
        lista_servicios[item].sort()

    # # Obtener todas las fechas
    # fechas_servicios = []
    # for item in lista_servicios.keys():
    #     fechas_servicios.extend(lista_servicios[item])

    # fechas = list(set(fechas_servicios))

    # # Guardar fechas y sus servicios
    # lista_insert = []
    # lista_update = []
    # fechas_db = colleccion_calendario.distinct("_id", {"_id": {"$in": fechas}})

    # for fecha in fechas:
    #     servicios = []
    #     for item in lista_servicios.keys():
    #         if fecha in lista_servicios[item]:
    #             servicios.append(item)

    #     if not fecha in fechas_db:
    #         lista_insert.append({
    #             "_id": fecha,
    #             "fecha": fecha,
    #             "servicios": servicios
    #         })
    #     else:
    #         lista_update.append(UpdateOne({"_id": fecha}, {"$push": {"servicios": {"$each": servicios}}}))
    
    # if len(lista_insert) > 0:
    #     colleccion_calendario.insert_many(lista_insert)
    # if len(lista_update) > 0:
    #     colleccion_calendario.bulk_write(lista_update)

    # Viaje.fechas
    lista_updates = []
    for viaje_key in lista_viajes.keys():
        viaje: dict = lista_viajes[viaje_key]
        lista_updates.append(UpdateOne({"_id": viaje_key}, {"$set": {"fechas": lista_servicios.get(viaje["service_id"])}}))
    if (len(lista_updates) > 0):
        colleccion_viajes.bulk_write(lista_updates)

    # Eliminar viajes que no contengan fecha (realizados en el pasado)
    colleccion_viajes.delete_many({"fechas": {"$type": "null"}})
    colleccion_viajes.delete_many({"fechas": {"$size": 0}})
    #endregion

    ## Feed.info
    lista_updates = []
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "feed_info.txt"))):
        lista_info = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "feed_info.txt"))
        colleccion_feed = db["feeds"]

        if len(lista_info) > 0:
            info = lista_info[0]
            doc = {
                "nombreEditor": info.get("feed_publisher_name"),
                "urlEditor": info.get("feed_publisher_url"),
                "idioma": info.get("feed_lang"),
                "idiomaPredeterminado": none_si_vacio(info.get("default_lang", "")),
                "fechaInicio": datetime.strptime(info.get("feed_start_date"), "%Y%m%d") if none_si_vacio(info.get("feed_start_date", "")) is not None else None,
                "fechaFin": datetime.strptime(info.get("feed_end_date"), "%Y%m%d") if none_si_vacio(info.get("feed_end_date", "")) is not None else None,
                "version": none_si_vacio(info.get("feed_version", "")),
                "email": none_si_vacio(info.get("feed_contact_email", "")),
                "urlContacto": none_si_vacio(info.get("feed_contact_url", ""))
            }
            lista_updates.append(UpdateOne({"_id": gtfs["idFeed"]}, {"$set": {"info": doc}}))

    ## Feed.atribuciones
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "attributions.txt"))):
        lista_atribuciones = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "attributions.txt"))

        atribuciones = []
        for atribucion in lista_atribuciones:
            atribuciones.append({
                "idAtribucion": none_si_vacio(atribucion.get("attribution_id", "")),
                "idAgencia": none_si_vacio(atribucion.get("agency_id", "")),
                "idLinea": none_si_vacio(atribucion.get("route_id", "")),
                "idViaje": none_si_vacio(atribucion.get("trip_id", "")),
                "nombreOrganizacion": none_si_vacio(atribucion.get("organization_name", "")),
                "esProductor": atribucion.get("is_producer", "0") == "1",
                "esOperador": atribucion.get("is_operator", "0") == "1",
                "esAutoridad": atribucion.get("is_authority", "0") == "1",
                "url": none_si_vacio(atribucion.get("attribution_url", "")),
                "email": none_si_vacio(atribucion.get("attribution_email", "")),
                "telefono": none_si_vacio(atribucion.get("attribution_phone", ""))
            })

        lista_updates.append(UpdateOne({"_id": gtfs["idFeed"]}, {"$set": {"atribuciones": atribuciones}}))
    
    if len(lista_updates) > 0:
        colleccion_feed.bulk_write(lista_updates)

    # ## Traduccion
    # if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "translations.txt"))):
    #     lista_traducciones = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "translations.txt"))
    #     colleccion_traducciones = db["traducciones"]

    #     lista_documentos = []
    #     for traduccion in lista_traducciones:
    #         lista_documentos.append({
    #             "nombreTabla": traduccion.get("table_name"),
    #             "nombreCampo": traduccion.get("field_name"),
    #             "idioma": traduccion.get("language"),
    #             "traduccion": traduccion.get("translation"),
    #             "idElemento": none_si_vacio(traduccion.get("record_id", "")),
    #             "idElemento2": none_si_vacio(traduccion.get("record_sub_id", "")),
    #             "valorOriginal": none_si_vacio(traduccion.get("field_value", ""))
    #         })
    #     colleccion_traducciones.insert_many(lista_documentos)

    db["feeds"].update_one({"_id": gtfs["idFeed"]}, {"$set": {"actualizar.db": False}})


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
    start = datetime.now()

    cliente = conectar()
    db = cliente["gtfs"]

    feeds_eliminar_ids = db["feeds"].distinct("idFeed", {"$or": [{"actualizar.db": True}, {"eliminar": True}]})
    db["feeds"].delete_many({"eliminar": True})
    for colleccion in ["agencias", "lineas", "paradas", "traducciones", "viajes"]:
        db[colleccion].delete_many({"_id": {"$regex": f"^({('|'.join(feeds_eliminar_ids))})_"}})

    # db["calendario"].update_many({}, {
    #     "$pull": {
    #         "servicios": {
    #             "$regex": f"^{('|'.join(feeds_eliminar_ids))}_"
    #         }
    #     }
    # })
    # db["calendario"].delete_many({"servicios": []})

    for feed in db["feeds"].find({"actualizar.db": True}):
        try:
            print(f"Subiendo {feed['idFeed']}...")
            guardar(feed, db)
        finally:
            pass

    print(f"Acabado en {(datetime.now()-start).total_seconds()}s")


if __name__ == '__main__':
    main()