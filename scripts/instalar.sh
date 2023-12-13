### TIPPECANOE ###
# https://github.com/mapbox/tippecanoe#installation
sudo apt-get install build-essential libsqlite3-dev zlib1g-dev

sudo apt-get install git

git clone https://github.com/felt/tippecanoe.git
cd tippecanoe
make -j
sudo make install
cd ..
rm tippecanoe -rf