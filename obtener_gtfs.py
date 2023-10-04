import requests
import zipfile
import os
import shutil
import json
import csv
from datetime import datetime

config = {}
directorio_zip = ""
directorio_gtfs = ""


archivos_agency_id = ["agency.txt", "routes.txt", "fare_attributes.txt"] # Archivos que utilizan agency_id No se incluye attributions


def descargar(gtfs, config):
    archivo_zip = os.path.join(directorio_zip, gtfs["id"]+".zip")
    for descarga in gtfs["sources"]:
        error = False
        if (descarga["type"] == "HTTP"):
            respuesta = requests.request("GET", descarga["url"])

            if (respuesta.status_code == 200):
                print("HTTP")
                with open(archivo_zip, 'wb') as f:
                    f.write(respuesta.content)
            else:
                error = True
        elif (descarga["type"] == "NAP_MITMA"):
            # Obtener información del conjunto de datos
            url_info = f"https://nap.mitma.es/api/Fichero/{descarga['conjuntoDatoId']}"
            headers = {"ApiKey": config.get("nap_mitma_api_key")}
            info_conjunto = requests.request("GET", url_info, headers=headers)
            if (info_conjunto.status_code == 200):
                info_conjunto = info_conjunto.json()
                #TODO: Comprobar si el conjunto de datos es correcto
                # Descargar fichero
                url_descarga = f"https://nap.mitma.es/api/Fichero/download/{info_conjunto.get('ficherosDto')[0].get('ficheroId')}"
                fichero = requests.request("GET", url_descarga, headers=headers)
                if (fichero.status_code == 200):
                    print("NAP MITMA")
                    with open(archivo_zip, 'wb') as f:
                        f.write(fichero.content)
            else:
                error = True
        
        if not error:
            break


def descomprimir(gtfs):
    with zipfile.ZipFile(os.path.join(directorio_zip, gtfs["id"]+".zip"), 'r') as zip_ref:
        if os.path.exists(os.path.join(directorio_gtfs, gtfs["id"])):
            shutil.rmtree(os.path.join(directorio_gtfs, gtfs["id"]))
        zip_ref.extractall(os.path.join(directorio_gtfs, gtfs["id"]))


def actualizar_ids(gtfs):
    # Obtener agency_id de agency.txt en caso de contener una sola agencia y no ser necesario referenciar agency_id en otros archivos
    agency_id_unico = None # Se usará para almacenar el agency_id en caso de que solo haya una agencia en el feed
    if "agency.txt" in os.listdir(os.path.join(directorio_gtfs, gtfs["id"])):
        with open(os.path.join(directorio_gtfs, gtfs["id"], "agency.txt"), 'r', encoding='UTF-8') as f:
            reader = csv.DictReader(f)
            agencias = []
            for fila in reader:
                agencias.append(fila)
            
            if len(agencias) == 1:
                agency_id_unico = (gtfs["id"] + '_' + agencias[0]["agency_id"]) if "agency_id" in reader.fieldnames else gtfs["id"]

    # Abrir todos los archivos del directorio y actualizar ids
    lista_archivos = os.listdir(os.path.join(directorio_gtfs, gtfs["id"]))
    for file in lista_archivos:
        # Archivos
        original = os.path.join(directorio_gtfs, gtfs["id"], file)
        nuevo = os.path.join(directorio_gtfs, gtfs["id"], file + 'tmp')

        # Leer el archivo
        with open(original, 'r', encoding='UTF-8') as f_in, open(nuevo, 'w', newline='', encoding='UTF-8') as f_out:
            reader = csv.DictReader(f_in)
            
            headers = set(reader.fieldnames)
            anadir_agency_id = False
            if file in archivos_agency_id and "agency_id" not in reader.fieldnames:
                # Incluir siempre agency_id
                headers.add("agency_id")
                anadir_agency_id = True
            
            writer = csv.DictWriter(f_out, fieldnames=headers)
            writer.writeheader()
            
            columnas_id = [col for col in reader.fieldnames if col[-3:] == "_id" or col == "parent_station"]
            for fila in reader: # Recorrer cada fila
                for col in columnas_id: # Actualizar ids con el prefijo del gtfs
                    fila[col] = gtfs["id"] + "_" + fila[col]
                if anadir_agency_id:
                    fila["agency_id"] = agency_id_unico if agency_id_unico != None else gtfs["id"]
                writer.writerow(fila)
        
        os.remove(original)
        os.rename(nuevo, original)


def main():
    global config, directorio_zip, directorio_gtfs
    start = datetime.now()
    with open('config.json') as f:
        config = json.load(f)

    directorio_zip = os.path.join(os.getcwd(), config["directorio_zip"])
    directorio_gtfs = os.path.join(os.getcwd(), config["directorio_gtfs"])

    try:
        os.mkdir(directorio_zip)
        os.mkdir(directorio_gtfs)
    except FileExistsError:
        pass

    feeds = []
    with open(os.path.join(os.getcwd(), config["feeds"])) as f:
        feeds = json.load(f)

    for feed in feeds:
        descargar(feed, config)
        descomprimir(feed)
        actualizar_ids(feed)

    print(f"Acabado en {(datetime.now()-start).total_seconds()}s")


if __name__ == '__main__':
    main()