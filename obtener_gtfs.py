import urllib.request
import zipfile
import os
import json

config = {}
directorio_zip = ""
directorio_gtfs = ""


def descargar(gtfs):
    urllib.request.urlretrieve(gtfs["url"], os.path.join(directorio_zip, gtfs["archivo"]))


def descomprimir(gtfs):
    with zipfile.ZipFile(os.path.join(directorio_zip, gtfs["archivo"]), 'r') as zip_ref:
        zip_ref.extractall(os.path.join(directorio_gtfs, gtfs["directorio"]))


def main():
    global config, directorio_zip, directorio_gtfs
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
        descargar(feed)
        descomprimir(feed)


if __name__ == '__main__':
    main()