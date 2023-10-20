import hashlib
from pathlib import Path
from typing import List
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
from dotenv import load_dotenv


config = {}
directorio_zip = ""
directorio_gtfs = ""
archivos_agency_id = ["agency.txt", "routes.txt", "fare_attributes.txt"] # Archivos que utilizan agency_id (No se incluye attributions)


def conectar() -> MongoClient:
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        # Prod
        uri = f"mongodb://{os.environ['MONGODB_SERVER_USER']}:{os.environ['MONGODB_SERVER_USER_PASSWORD']}@127.0.0.1:27017/{os.environ['MONGODB_INITDB_DATABASE']}"
    else:
        #TODO: Quitar (Solo para pruebas)
        uri = f"mongodb://serverUser:serverUser@192.168.1.10:27017/gtfs"
    
    cliente = MongoClient(uri, server_api=ServerApi('1'))

    return cliente


def sincronizar_feeds(db_feeds: Collection[_DocumentType], file_feeds: List[dict]):
    # Obtener idFeeds de feeds en la base de datos y en el fichero
    db_feeds_idFeeds = db_feeds.distinct("idFeed")
    file_feeds_idFeeds = [feed["idFeed"] for feed in file_feeds]
    feeds_nuevos = [feed for feed in file_feeds if feed["idFeed"] not in db_feeds_idFeeds]

    # Eliminar feeds que no existen en el fichero
    if len(db_feeds_idFeeds) > 0:
        db_feeds.delete_many({"idFeed": {"$nin": file_feeds_idFeeds}})

    # Añadir feeds que no existen en la base de datos
    if len(feeds_nuevos) > 0:
        db_feeds.insert_many(feeds_nuevos)


def descargar(gtfs: dict, config: dict, db_feeds: Collection[_DocumentType]) -> bool:
    actualizar = False
    archivo_zip = os.path.join(directorio_zip, gtfs["idFeed"]+".zip")
    for descarga in gtfs["sources"]:
        error = False
        print(gtfs["idFeed"])
        if (descarga["type"] == "HTTP"):
            print("HTTP")
            respuesta = requests.request("GET", descarga["url"])

            if (respuesta.status_code == 200):
                # Comprobar MD5
                md5 = hashlib.md5(respuesta.content).hexdigest()
                if (md5 != gtfs.get("MD5", "")):
                    with open(archivo_zip, 'wb') as f:
                        f.write(respuesta.content)
                    # Actualizar MD5
                    db_feeds.update_one({"idFeed": gtfs["idFeed"]}, {"$set": {"MD5": md5, "actualizar": True}})
                    actualizar = True
            else:
                error = True
        elif (descarga["type"] == "GEOEUSKADI"):
            print("GEOEUSKADI")
            # Usar metodo HEAD para obtener solamente el encabezado
            encabezado = requests.request("HEAD", descarga["url"])

            if (encabezado.status_code == 200):
                # Comprobar si el valor etag ha cambiado
                if (encabezado.headers.get("etag") != gtfs.get("etag", "")):
                    # Si ha cambiado, descargar el archivo
                    respuesta = requests.request("GET", descarga["url"])

                    if (respuesta.status_code == 200):
                        with open(archivo_zip, 'wb') as f:
                            f.write(respuesta.content)

                        # Actualizar etag
                        db_feeds.update_one({"idFeed": gtfs["idFeed"]}, {"$set": {"etag": respuesta.headers.get("etag"), "actualizar": True}})
                        actualizar = True  
                    else:
                        error = True
            else:
                error = True
        elif (descarga["type"] == "NAP_MITMA"):
            print("NAP_MITMA")
            # Obtener información del conjunto de datos
            url_info = f"https://nap.mitma.es/api/Fichero/{descarga['conjuntoDatoId']}"
            headers = {"ApiKey": config.get("nap_mitma_api_key"), "Accept": "application/json"}
            info_conjunto = requests.request("GET", url_info, headers=headers)
            print(info_conjunto)
            if (info_conjunto.status_code == 200):
                info_conjunto = info_conjunto.json()
                print(info_conjunto.get('ficherosDto', [{}])[0].get('fechaActualizacion'))
                #TODO: Comprobar si el conjunto de datos es correcto
                if (info_conjunto.get('ficherosDto', [{}])[0].get('fechaActualizacion', "") != gtfs.get("fechaActualizacion", "")):
                    # Descargar fichero
                    url_descarga = f"https://nap.mitma.es/api/Fichero/download/{info_conjunto.get('ficherosDto', [{}])[0].get('ficheroId')}"
                    fichero = requests.request("GET", url_descarga, headers=headers)
                    print(fichero.headers)
                    if (fichero.status_code == 200):
                        with open(archivo_zip, 'wb') as f:
                            f.write(fichero.content)

                        # Actualizar fecha de actualización
                        db_feeds.update_one({"idFeed": gtfs["idFeed"]}, {"$set": {"fechaActualizacion": info_conjunto.get('ficherosDto', [{}])[0].get('fechaActualizacion'), "actualizar": True}})
                        actualizar = True
                    else:
                        error = True
            else:
                error = True
        
        if not error:
            break
    return actualizar


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
            
            columnas_id = [col for col in reader.fieldnames if col[-3:] == "_id" or col == "parent_station"]
            contador_orden = 0
            for fila in reader: # Recorrer cada fila
                for col in reader.fieldnames:
                    fila[col] = fila[col].strip() if fila[col] != None else fila[col] # Limpiar columna de espacio innecesarios
                    if col in columnas_id and (col != "parent_station" or fila[col] != ''): # La columna es un id y si es parent_station no está vacía
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

    # Cargar configuración de MongoDB
    load_dotenv(dotenv_path=Path('./mongodb/mongodb.env'))
    with open('config.json') as f:
        config = json.load(f)

    # Obtener y generar directorios
    directorio_zip = os.path.join(os.getcwd(), config["directorio_zip"])
    directorio_gtfs = os.path.join(os.getcwd(), config["directorio_gtfs"])

    try:
        os.mkdir(directorio_zip)
        os.mkdir(directorio_gtfs)
    except FileExistsError:
        pass

    # Conectar a MongoDB
    cliente = conectar()
    if not os.environ.get('MONGODB_SERVER_USER') is None:
        db = cliente[os.environ['MONGODB_INITDB_DATABASE']]
    else:
        db = cliente["gtfs"]

    feeds = []
    with open(os.path.join(os.getcwd(), config["feeds"])) as f:
        feeds = json.load(f)

    sincronizar_feeds(db["feeds"], feeds)

    for feed in db["feeds"].find():
        actualizar = descargar(feed, config, db["feeds"])
        if actualizar:
            descomprimir(feed)
            adaptar_datos(feed)

    print(f"Acabado en {(datetime.now()-start).total_seconds()}s")


if __name__ == '__main__':
    main()