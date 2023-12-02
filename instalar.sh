# https://github.com/mapbox/tippecanoe#installation

# Script para Ubuntu 20.04

# Instalar PIP
sudo apt install python3-pip -y

# Instalar requirements.txt
sudo pip3 install -r ./requirements.txt -r ./api/requirements.txt

### TIPPECANOE ###
sudo apt-get install build-essential libsqlite3-dev zlib1g-dev

sudo apt-get install git

git clone https://github.com/felt/tippecanoe.git
cd tippecanoe
make -j
sudo make install
cd ..
rm tippecanoe -rf