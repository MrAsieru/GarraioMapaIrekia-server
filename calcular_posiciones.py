import os
import json
import csv
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from pymongo import InsertOne
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.typings import _DocumentType
from datetime import datetime, date, timedelta, time
from shapely.geometry import LineString, Point
from shapely import to_geojson
import pytz

config = {}
directorio_gtfs = ""
directorio_geojson = ""

PRECISION_COORDENADAS = 6 # Precisión de las coordenadas


def conectar():
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        # Prod
        uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@127.0.0.1:27017/{os.environ['MONGODB_INITDB_DATABASE']}"
    else:
        #TODO: Quitar (Solo para pruebas)
        uri = f"mongodb://serverUser:serverUser@192.168.1.10:27017/gtfs"
    
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente


def calcular(gtfs: dict, db: Database[_DocumentType]):
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt"))):
        print(gtfs["idFeed"])
        lista_agencias = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "agency.txt"), ["agency_id"])
        lista_lineas = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "routes.txt"), ["route_id"])
        lista_servicios = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "trips.txt"), ["service_id"])
        lista_recorridos = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "shapes.txt"), ["shape_id"])
        lista_horarios = csv_to_listdict(os.path.join(directorio_gtfs, gtfs["idFeed"], "stop_times.txt"), ["trip_id"])
        lista_paradas = csv_to_dict(os.path.join(directorio_gtfs, gtfs["idFeed"], "stops.txt"), ["stop_id"])

        servicios_fechas = obtener_servicios_fechas(gtfs)

        lista_update = []

        # Recorrer todos los viajes de cada servicio
        inicio_calculos = datetime.now()
        for servicio in servicios_fechas.keys():
            print(f"\tCalculando posiciones de: {servicio} {datetime.now().isoformat()}")

            # Obtener diccionario con todas las posiciones de cada viaje (fecha -> agencia -> viaje -> posiciones codificadas) 
            lista_para_subir = {}

            for viaje in lista_servicios.get(servicio, []):
                try:
                    # Obtener horario (viaje) y zona horaria (agencia)
                    horario = lista_horarios[viaje["trip_id"]]
                    zonaHoraria = lista_agencias[lista_lineas[viaje["route_id"]]["agency_id"]]["agency_timezone"]

                    fechas_posiciones_codificadas = codificar_posiciones(posiciones_de_viaje(lista_recorridos[viaje["shape_id"]], horario, lista_paradas))

                    # Recorrer todas las fechas en las que hay servicio
                    for fecha_servicio in servicios_fechas[servicio]:
                        # Recorrer todas las fechas en las que hay datos
                        for fecha in fechas_posiciones_codificadas.keys():
                            # Obtener fecha con zona horaria (agencia)
                            fecha_zona_horaria = pytz.timezone(zonaHoraria).localize(fecha.replace(year=fecha_servicio.year, month=fecha_servicio.month, day=fecha_servicio.day))
                            
                            # Convertir fecha a UTC
                            fecha_utc = fecha_zona_horaria.astimezone(pytz.utc)

                            if not fecha_utc in lista_para_subir.keys():
                                lista_para_subir[fecha_utc] = {}

                            if not lista_lineas[viaje["route_id"]]["agency_id"] in lista_para_subir[fecha_utc].keys():
                                lista_para_subir[fecha_utc][lista_lineas[viaje["route_id"]]["agency_id"]] = {}
                            
                            lista_para_subir[fecha_utc][lista_lineas[viaje["route_id"]]["agency_id"]][viaje["trip_id"]] = fechas_posiciones_codificadas[fecha]
                except:
                    pass

            if len(lista_para_subir.keys()) > 0:
                def generador_viajes(viajes: dict):
                    for viaje in viajes.keys():
                        yield {
                            "idViaje": viaje,
                            "posiciones": viajes[viaje]
                        }
                # Crear documentos
                lista_update.clear()
                for fecha in lista_para_subir.keys():
                    for agencia in lista_para_subir.get(fecha, {}).keys():
                        lista_update.append(InsertOne({"fecha": fecha, "idAgencia": agencia, "idServicio": servicio, "viajes": list(generador_viajes(lista_para_subir[fecha][agencia]))}))

                # Subir documentos
                inicio_subida = datetime.now()
                tamano_batch = 1000
                for i in range(0, len(lista_update), tamano_batch):
                    db["posiciones"].bulk_write(lista_update[i:i+tamano_batch])
                print(f"\t\tSubido en {(datetime.now()-inicio_subida).total_seconds()}s")
            
        print(f"\tCalculado en {(datetime.now()-inicio_calculos).total_seconds()}s")

    db["feeds"].update_one({"_id": gtfs["idFeed"]}, {"$set": {"actualizar.posiciones": False}})


def posiciones_de_viaje(recorrido: List[dict], horario: List[dict], paradas: List[dict]) -> List[dict]:
    # Obtener las posiciones por cada segundo, de minuto en minuto

    shape_id = recorrido[0]["shape_id"]

    # Ordenar recorrido y paradas
    recorrido.sort(key=lambda x: int(x["shape_pt_sequence"]))
    horario.sort(key=lambda x: int(x["stop_sequence"]))

    # Dividir recorrido en tramos entre paradas
    tramos = []
    recorrido_restante = LineString([[float(punto["shape_pt_lon"]), float(punto["shape_pt_lat"])] for punto in recorrido])

    # Dividir recorrido en tramos entre paradas
    for i in range(1, len(horario) - 1): # El tramo final se añade fuera del bucle (-1)
        horario_parada = horario[i]
        parada = paradas[horario_parada["stop_id"]]

        d = parada.get("distancia", {}).get(shape_id, None)
        if d is None:
            # Obtener punto del recorrido más cercano a la parada
            posicion_parada = Point(float(parada["stop_lon"]), float(parada["stop_lat"]))

            # Obtener distancia a lo largo del recorrido hasta la parada
            d = recorrido_restante.project(posicion_parada)

            # Guardar distancia del recorrido en la parada para usarla posteriormente
            if not "distancia" in parada.keys():
                parada["distancia"] = {}
            parada["distancia"][shape_id] = d
        
        # Dividir recorrido en dos tramos
        tramo, recorrido_restante = cut(recorrido_restante, d)
        tramos.append(tramo)
    
    # Añadir el tramo final
    tramos.append(recorrido_restante)

    posiciones = []

    # Obtener posiciones durante la primera parada
    posiciones.extend(posiciones_en_parada(horario[0], paradas, tramos[0]))

    # Recorrer cada tramo
    for i in range(len(tramos)):
        # Duración (segundos) del tramo junto con la hora inicial
        duracion, hora_salida = tiempo_entre_paradas(horario[i], horario[i+1])

        # Distancia recorrida cada segundo
        distancias = [tramos[i].length * j / (duracion - 1) for j in range(duracion)]

        # Obtener posiciones durante el tramo
        j = 0
        for punto in [tramos[i].interpolate(distancia) for distancia in distancias]:
            posiciones.append({
                "lat": punto.y,
                "lon": punto.x,
                "fecha": hora_salida + timedelta(seconds=j),
                "proximoOrdenParada": horario[i]["stop_sequence"]
            })
            j += 1

        if i < len(tramos) - 1:
            # Obtener posiciones durante la parada
            posiciones.extend(posiciones_en_parada(horario[i+1], paradas, tramos[i+1]))

    # Asegurarse de que está ordenado
    posiciones.sort(key=lambda x: x["fecha"])

    # Dividir posiciones por fecha (cada minuto)
    fechas_posiciones = {}
    for posicion in posiciones:
        # Conseguir fecha sin segundos
        fecha_minuto = posicion["fecha"].replace(second=0, microsecond=0)

        # Añadir fecha a diccionario si no existe y rellenar valores vacios hasta el segundo de fecha
        if not fecha_minuto in fechas_posiciones.keys():
            fechas_posiciones[fecha_minuto] = []

            # Rellenar valores vacios hasta el segundo de fecha
            segundo_inicial = posicion["fecha"].second
            for i in range(segundo_inicial):
                fechas_posiciones[fecha_minuto].append(None)
        
        fechas_posiciones[fecha_minuto].append(posicion)

    return fechas_posiciones


def codificar_posiciones(fechas_posiciones: dict) -> dict:
    # Codificar valores
    # Formato: lat|lon|proximoOrdenParada~lat|lon|proximoOrdenParada~...
    # ~: separa posiciones, |: separa lat, lon y proximoOrdenParada, @: datos vacios
    posiciones_codificadas = {}
    for fecha in fechas_posiciones.keys():
        datos = ""
        primera_posicion = True
        for i in range(len(fechas_posiciones[fecha])):
            if fechas_posiciones[fecha][i] is None:
                datos += "@|@|@"
            elif primera_posicion:
                datos += str(round(fechas_posiciones[fecha][i]["lat"], PRECISION_COORDENADAS)) + "|" + str(round(fechas_posiciones[fecha][i]["lon"], PRECISION_COORDENADAS)) + "|" + fechas_posiciones[fecha][i].get("proximoOrdenParada", "")
                primera_posicion = False
            else:
                dif_lat = int(round(fechas_posiciones[fecha][i]["lat"] - fechas_posiciones[fecha][i-1]["lat"], PRECISION_COORDENADAS) * (10 ** PRECISION_COORDENADAS))
                dif_lon = int(round(fechas_posiciones[fecha][i]["lon"] - fechas_posiciones[fecha][i-1]["lon"], PRECISION_COORDENADAS) * (10 ** PRECISION_COORDENADAS))
                datos += str(dif_lat) + "|" + str(dif_lon) + "|" + fechas_posiciones[fecha][i].get("proximoOrdenParada", "")

            if i < len(fechas_posiciones[fecha]) - 1:
                datos += "~"
        
        posiciones_codificadas[fecha] = datos

    return posiciones_codificadas


def posiciones_en_parada(horario_parada: dict, lista_paradas: List[dict], tramo: LineString) -> List[dict]:
    posiciones = []

    j = 0
    duracion, hora_llegada = tiempo_en_parada(horario_parada)
    if duracion > 0:
        posicion_parada = Point(float(lista_paradas[horario_parada["stop_id"]]["stop_lon"]), float(lista_paradas[horario_parada["stop_id"]]["stop_lat"]))
        d = tramo.project(posicion_parada)
        posicion_parada = tramo.interpolate(d)
    for j in range(duracion):
        posiciones.append({
            "lat": posicion_parada.y,
            "lon": posicion_parada.x,
            "fecha": hora_llegada + timedelta(seconds=j),
            "proximoOrdenParada": horario_parada["stop_sequence"]
        })
    
    return posiciones


def cut(line, distance):
    # This is taken from shapely manual
    # Cuts a line in two at a distance from its starting point
    if distance <= 0.0 or distance >= line.length:
        return (LineString(line), LineString())
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return (LineString(coords[:i+1]), LineString(coords[i:]))
        if pd > distance:
            cp = line.interpolate(distance)
            return (LineString(coords[:i] + [(cp.x, cp.y)]), LineString([(cp.x, cp.y)] + coords[i:]))

        
def tiempo_entre_paradas(parada1: dict, parada2: dict) -> (int, datetime):
    if "departure_time" in parada1.keys():
        hora_salida = parada1["departure_time"]
    else:
        hora_salida = parada1["arrival_time"]
    if "arrival_time" in parada2.keys():
        hora_llegada = parada2["arrival_time"]
    else:
        hora_llegada = parada2["departure_time"]
    
    hora_salida = hora_salida.split(":")
    hora_llegada = hora_llegada.split(":")

    hora_salida = datetime(year=1970, month=1, day=1, hour=int(hora_salida[0]) % 24, minute=int(hora_salida[1]), second=int(hora_salida[2])) + timedelta(days=int(hora_salida[0]) // 24)
    hora_llegada = datetime(year=1970, month=1, day=1, hour=int(hora_llegada[0]) % 24, minute=int(hora_llegada[1]), second=int(hora_llegada[2])) + timedelta(days=int(hora_llegada[0]) // 24)
    duracion = hora_llegada - hora_salida
    return duracion.seconds, hora_salida


def tiempo_en_parada(parada: dict) -> (int, datetime):
    if parada.get("arrival_time", "") != "" and parada.get("departure_time", "") != "":
        hora_llegada = parada["arrival_time"]
        hora_salida = parada["departure_time"]
        
        hora_llegada = hora_llegada.split(":")
        hora_salida = hora_salida.split(":")

        hora_llegada = datetime(year=1970, month=1, day=1, hour=int(hora_llegada[0]) % 24, minute=int(hora_llegada[1]), second=int(hora_llegada[2])) + timedelta(days=int(hora_llegada[0]) // 24)
        hora_salida = datetime(year=1970, month=1, day=1, hour=int(hora_salida[0]) % 24, minute=int(hora_salida[1]), second=int(hora_salida[2])) + timedelta(days=int(hora_salida[0]) // 24)
        duracion = hora_salida - hora_llegada
        return duracion.seconds, hora_llegada
    else:
        return 0, None


def obtener_servicios_fechas(gtfs: dict) -> List[date]:
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar.txt"))): 
        lista_calendario = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar.txt"))
    else:
        lista_calendario = []
    if (os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar_dates.txt"))):
        lista_fechas_calendario = csv_to_list(os.path.join(directorio_gtfs, gtfs["idFeed"], "calendar_dates.txt"))
    else:
        lista_fechas_calendario = []

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
    
    # Eliminar servicios sin fechas
    for item in list(lista_servicios.keys()):
        if len(lista_servicios[item]) == 0:
            del lista_servicios[item]

    # Eliminar posibles fechas repetidas
    for item in lista_servicios.keys():
        lista_servicios[item] = list(set(lista_servicios[item]))
        lista_servicios[item].sort()
    
    return lista_servicios


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
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        db = cliente[os.environ['MONGODB_INITDB_DATABASE']]
    else:
        #TODO: Quitar (Solo para pruebas)
        db = cliente["gtfs"]

    # Eliminar posiciones de feeds que se van a actualizar
    feeds_actualizar_ids = db["feeds"].distinct("idFeed", {"$or": [{"actualizar.posiciones": True}, {"eliminar": True}]})
    ids_agencias = db["agencias"].distinct("_id", {"_id": {"$regex": f"^({('|'.join(feeds_actualizar_ids))})_"}})
    db["posiciones"].delete_many({"idAgencia": {"$in": ids_agencias}})

    try:
        for feed in db["feeds"].find({"actualizar.posiciones": True}):
            calcular(feed, db)
    finally:
        print(f"Acabado en {(datetime.now()-start).total_seconds()}s")


if __name__ == '__main__':
    main()