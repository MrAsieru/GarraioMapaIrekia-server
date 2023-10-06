#!/bin/sh
# Instalar PIP
sudo apt install python3-pip -y

# Instalar requirements.txt
sudo pip3 install -r requirements.txt

# Establecer variables de entorno
source ./mongodb/mongodb.env

# Iniciar servidor Martin
sudo docker compose up -d