import os
import json
import requests
from typing import List


def conseguir_regiones(config: dict) -> List[dict]:
    regiones = {}

    url_info = f"https://nap.mitma.es/api/Fichero/"
    headers = {"ApiKey": config.get("nap_mitma_api_key")}
    regiones_request = requests.request("GET", url_info, headers=headers)
    if (regiones_request.status_code == 200):
        regiones_request = regiones_request.json()
        for region in regiones_request:
            regiones[region.get('tipoNombre')] = {"nombre": region.get('nombre'), "regionId": region.get('regionId')}
    
    return regiones


def conseguir_feeds_de_provincias(provincias: List[int], config: dict) -> List[dict]:
    feed = []

    url = "https://nap.mitma.es/api/Fichero/Filter"

    payload = {"provincias": provincias}
    headers = {"ApiKey": config.get("nap_mitma_api_key")}

    response = requests.request("POST", url, json=payload, headers=headers)

    if (response.status_code == 200):
        response = response.json()
        for feed_nap in response.get("conjuntosDatoDto", []):
            feed.append({"id": feed_nap.get("nombre", ""), "sources": [{"type": "NAP_MITMA", "conjuntoDatoId": feed_nap.get("conjuntoDatoId", "")}]})

    return feed


def main():
    with open('config.json') as f:
        config = json.load(f)

    print(json.dumps(conseguir_feeds_de_provincias([48,1,20], config)))
          

if __name__ == "__main__":
    main()