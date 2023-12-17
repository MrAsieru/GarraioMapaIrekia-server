import hashlib
import sys
from typing import List
from pymongo import UpdateOne
import requests
import zipfile
import os
import shutil
import json
import csv
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.collection import Collection
from pymongo.server_api import ServerApi
from pymongo.typings import _DocumentType

config = {}
directorio_zip = "/server/zip"
directorio_gtfs = "/server/gtfs"
archivos_agency_id = ["agency.txt", "routes.txt", "fare_attributes.txt"] # Archivos que utilizan agency_id (No se incluye attributions)


class MetodoDescarga:
    def __init__(self, feed: dict, fuente: dict, db_feeds: Collection[_DocumentType], archivo_zip: str):
        self.feed = feed
        self.fuente = fuente
        self.db_feeds = db_feeds
        self.archivo_zip = archivo_zip

    def descarga(self) -> (bool, bool):
        pass

    def guardar(self, contenido: bytes, info_db: dict, info_fuente: dict = None):
        with open(self.archivo_zip, 'wb') as f:
            f.write(contenido)

        # Almacenar datos en la base de datos
        info_base = {"actualizar": {"db": True, "tiles": True, "posiciones": True}}
        info_base.update(info_db)
        lista_update = []
        lista_update.append(UpdateOne({"_id": self.feed["idFeed"]}, {"$set": info_base}))
        
        # Actualizar información de la fuente
        if info_fuente != None:
            for atr in info_fuente.keys():
                lista_update.append(UpdateOne(
                    {"_id": self.feed["idFeed"], "fuentes": {"$elemMatch": {"url": self.fuente.get("url", None), "conjuntoDatoId": self.fuente.get("conjuntoDatoId", None)}}},
                    {"$set": {f"fuentes.$.{atr}": info_fuente[atr]}}
                ))
        self.db_feeds.bulk_write(lista_update)


class MetodoDescargaHTTP(MetodoDescarga):
    def __init__(self, feed: dict, fuente: dict, db_feeds: Collection[_DocumentType], archivo_zip: str):
        super().__init__(feed, fuente, db_feeds, archivo_zip)
        print("\tProbando mediante HTTP")
    
    def descarga(self) -> (bool, bool):
        error = True
        actualizar = False
        try:
            respuesta = requests.request("GET", self.fuente["url"])
            if (respuesta.status_code == 200):
                # Comprobar MD5
                md5 = hashlib.md5(respuesta.content).hexdigest()
                if (md5 != self.feed.get("md5", "")):
                    super().guardar(respuesta.content, {"md5": md5})
                    error = False
                    actualizar = True
                else:
                    error = False
        finally:
            return error, actualizar


class MetodoDescargaETAG(MetodoDescarga):
    def __init__(self, feed: dict, fuente: dict, db_feeds: Collection[_DocumentType], archivo_zip: str):
        super().__init__(feed, fuente, db_feeds, archivo_zip)
        print("\tProbando mediante HTTP (Etag)")
    
    def descarga(self) -> (bool, bool):
        error = True
        actualizar = False
        try:
            # Usar metodo HEAD para obtener solamente el encabezado
            encabezado = requests.request("HEAD", self.fuente["url"])
            if (encabezado.status_code == 200):
                # Comprobar si el valor etag ha cambiado
                if (encabezado.headers.get("etag") != self.fuente.get("etag", "")):
                    # Si ha cambiado, descargar el archivo
                    respuesta = requests.request("GET", self.fuente["url"])

                    if (respuesta.status_code == 200):
                        # Comprobar MD5
                        md5 = hashlib.md5(respuesta.content).hexdigest()
                        if (md5 != self.feed.get("md5", "")):
                            super().guardar(respuesta.content, {"md5": md5}, {"etag": encabezado.headers.get("etag")})
                            error = False
                            actualizar = True
                        else:
                            error = False
                else:
                    error = False
        finally:
            return error, actualizar


class MetodoDescargaNAPMITMA(MetodoDescarga):
    def __init__(self, feed: dict, fuente: dict, db_feeds: Collection[_DocumentType], archivo_zip: str):
        super().__init__(feed, fuente, db_feeds, archivo_zip)
        print("\tProbando mediante NAP MITMA")
    
    def descarga(self) -> (bool, bool):
        error = True
        actualizar = False
        try:
            # Obtener información del conjunto de datos
            url_info = f"https://nap.mitma.es/api/Fichero/{self.fuente['conjuntoDatoId']}"
            headers = {"ApiKey": config.get("napMitmaApiKey"), "Accept": "application/json"}
            info_conjunto = requests.request("GET", url_info, headers=headers)
            if (info_conjunto.status_code == 200):
                info_conjunto = info_conjunto.json()
                # Comprobar si el conjunto de datos es correcto 
                if (conjunto_correcto(info_conjunto)):
                    # Comprobar si la fecha de actualización ha cambiado
                    if (info_conjunto.get('ficherosDto', [{}])[0].get('fechaActualizacion', "") != self.fuente.get("fechaActualizacion", "")):
                        # Descargar fichero
                        url_descarga = f"https://nap.mitma.es/api/Fichero/download/{info_conjunto.get('ficherosDto', [{}])[0].get('ficheroId')}"
                        fichero = requests.request("GET", url_descarga, headers=headers)
                        if (fichero.status_code == 200):
                            # Comprobar MD5
                            md5 = hashlib.md5(fichero.content).hexdigest()
                            if (md5 != self.feed.get("md5", "")):
                                super().guardar(fichero.content, {"md5": md5}, {"fechaActualizacion": info_conjunto.get('ficherosDto', [{}])[0].get('fechaActualizacion')})
                                error = False
                                actualizar = True
                            else:
                                error = False
                    else:
                        error = False
        finally:
            return error, actualizar


def conectar() -> MongoClient:
    uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@mongodb:27017/gtfs"
    
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente


def sincronizar_feeds(colleccion_feeds: Collection[_DocumentType], file_feeds: List[dict]):
    # Obtener idFeeds de feeds en la base de datos y en el fichero
    db_feeds_idFeeds = colleccion_feeds.distinct("_id")
    file_feeds_idFeeds = [feed["idFeed"] for feed in file_feeds]
    feeds_nuevos = [feed for feed in file_feeds if feed["idFeed"] not in db_feeds_idFeeds]

    # Eliminar feeds que no existen en el fichero
    if len(db_feeds_idFeeds) > 0:
        colleccion_feeds.update_many({"_id": {"$nin": file_feeds_idFeeds}}, {"$set": {"eliminar": True}})

    # Actualizar fuentes de feeds existentes en la base de datos
    for feed in db_feeds_idFeeds:
        # Get existing fuentes from DB
        fuentes_db = colleccion_feeds.find_one({"_id": feed}, {"fuentes": 1})
        
        fuentes_nuevas = []
        for fuente in file_feeds[file_feeds_idFeeds.index(feed)]["fuentes"]:
            if len(tmp := list(filter(lambda x: x.get("url", 0) == fuente.get("url", 1) or x.get("conjuntoDatoId", 0) == fuente.get("conjuntoDatoId", 1), fuentes_db["fuentes"]))) > 0:
                fuentes_nuevas.append(tmp[0])
            else:
                fuentes_nuevas.append(fuente)

        colleccion_feeds.update_many({"_id": feed}, {"$set": {"fuentes": fuentes_nuevas}})

    # Añadir feeds que no existen en la base de datos
    if len(feeds_nuevos) > 0:
        for feed in feeds_nuevos:
            feed["_id"] = feed["idFeed"]
        colleccion_feeds.insert_many(feeds_nuevos)


def descargar(feed: dict, db_feeds: Collection[_DocumentType]) -> bool:
    actualizar = False
    archivo_zip = os.path.join(directorio_zip, feed["idFeed"]+".zip")
    print(feed["idFeed"])
    for fuente in feed["fuentes"]:
        descarga: MetodoDescarga = None
        if (fuente["tipo"] == "HTTP"):
            descarga = MetodoDescargaHTTP(feed, fuente, db_feeds, archivo_zip)            
        elif (fuente["tipo"] == "ETAG"):
            descarga = MetodoDescargaETAG(feed, fuente, db_feeds, archivo_zip)            
        elif (fuente["tipo"] == "NAP_MITMA"):
            descarga = MetodoDescargaNAPMITMA(feed, fuente, db_feeds, archivo_zip)
        else:
            continue

        error, actualizar = descarga.descarga()
        if not error:
            if actualizar:
                print("\tDescarga correcta")
            else:
                print("\tSin cambios")
            break
        else:
            print("\tDescarga incorrecta")
    return actualizar


def conjunto_correcto(conjunto: dict) -> bool:    
    return conjunto.get("ficherosDto", [{}])[0].get("validado", False)


def descomprimir(gtfs):
    with zipfile.ZipFile(os.path.join(directorio_zip, gtfs["idFeed"]+".zip"), 'r') as zip_ref:
        if os.path.exists(os.path.join(directorio_gtfs, gtfs["idFeed"])):
            shutil.rmtree(os.path.join(directorio_gtfs, gtfs["idFeed"]))
        zip_ref.extractall(os.path.join(directorio_gtfs, gtfs["idFeed"]))


# Asegurar IDs unicos para toda la aplicación y eliminar espacios innecesarios
def adaptar_datos(gtfs):
    # Obtener agency_id de agency.txt en caso de contener una sola agencia y no ser necesario referenciar agency_id en otros archivos
    agency_id_unico = None # Se usará para almacenar el agency_id en caso de que solo haya una agencia en el feed
    if "agency.txt" in os.listdir(os.path.join(directorio_gtfs, gtfs["idFeed"])):
        with open(os.path.join(directorio_gtfs, gtfs["idFeed"], "agency.txt"), 'r', encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            agencias = []
            for fila in reader:
                agencias.append(fila)
            
            if len(agencias) == 1:
                agency_id_unico = (gtfs["idFeed"] + '_' + agencias[0]["agency_id"]) if "agency_id" in reader.fieldnames else gtfs["idFeed"]

    # Abrir todos los archivos del directorio y actualizar ids
    lista_archivos = os.listdir(os.path.join(directorio_gtfs, gtfs["idFeed"]))
    for file in lista_archivos:
        # Archivos
        original = os.path.join(directorio_gtfs, gtfs["idFeed"], file)
        nuevo = os.path.join(directorio_gtfs, gtfs["idFeed"], file + 'tmp')

        # Leer el archivo
        with open(original, 'r', encoding="utf-8-sig") as f_in, open(nuevo, 'w', newline='', encoding="UTF-8") as f_out:
            reader = csv.DictReader(f_in)
            reader.fieldnames = [field.strip() for field in reader.fieldnames] # Algunos archivos tienen espacios entre las columnas, de esta manera se eliminan estos espacios
            
            headers = set(reader.fieldnames)
            # Añadir agency_id en caso de que no esté
            anadir_agency_id = False
            if file in archivos_agency_id and "agency_id" not in reader.fieldnames:
                # Incluir siempre agency_id
                headers.add("agency_id")
                anadir_agency_id = True
            
            writer = csv.DictWriter(f_out, fieldnames=headers)
            writer.writeheader()
            
            columnas_id = [col for col in reader.fieldnames if (col[-3:] == "_id" and col != "direction_id") or col == "parent_station"]
            contador_orden = 0
            for fila in reader: # Recorrer cada fila
                for col in reader.fieldnames:
                    fila[col] = fila[col].strip() if fila[col] != None else fila[col] # Limpiar columna de espacio innecesarios
                    if col in columnas_id and fila[col] != '': # La columna es un id y si es parent_station no está vacía
                        fila[col] = gtfs["idFeed"] + "_" + fila[col] # Actualizar ids con idFeed como prefijo
                    if col == "route_sort_order" and fila[col] == "": # Actualizar route_sort_order en caso de que no tenga valor
                        fila[col] = contador_orden
                        contador_orden += 1
                if anadir_agency_id:
                    fila["agency_id"] = agency_id_unico if agency_id_unico != None else gtfs["idFeed"]
                writer.writerow(fila)
        
        os.remove(original)
        os.rename(nuevo, original)


def main():
    global config, directorio_zip, directorio_gtfs
    start = datetime.now()

    with open('/server/config.json') as f:
        config = json.load(f)

    # Generar directorios
    try:
        os.mkdir(directorio_zip)
        os.mkdir(directorio_gtfs)
    except FileExistsError:
        pass

    # Conectar a MongoDB
    cliente = conectar()
    db = cliente["gtfs"]

    feeds = []
    with open("/server/feeds.json") as f:
        feeds = json.load(f)

    sincronizar_feeds(db["feeds"], feeds)

    for feed in db["feeds"].find({"eliminar": {"$ne": True}}):
        actualizar = descargar(feed, db["feeds"])
        sys.stdout.flush()
        if actualizar:
            descomprimir(feed)
            adaptar_datos(feed)

    print(f"Acabado en {(datetime.now()-start).total_seconds()}s")
    sys.stdout.flush()


if __name__ == '__main__':
    main()