FROM python:3.10.13-slim

# Instalar dependencias
RUN apt-get update && apt-get -y install git build-essential libsqlite3-dev zlib1g-dev cron ca-certificates curl gnupg
RUN install -m 0755 -d /etc/apt/keyrings && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && chmod a+r /etc/apt/keyrings/docker.gpg && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && apt-get update

# Instalar Docker
RUN apt-get update && apt-get -y install docker-ce-cli

# Instalar tippecanoe
RUN mkdir -p /tmp/tippecanoe-src
WORKDIR /tmp/tippecanoe-src
RUN git clone https://github.com/felt/tippecanoe.git
WORKDIR ./tippecanoe
RUN make -j && make install
WORKDIR /tmp
RUN rm -rf ./tmp/tippecanoe-src

# Copiar archivos
WORKDIR /server
ADD . /server
RUN chmod 755 scripts/*
RUN chmod 755 entrypoint.sh

# Instalar paquetes Python
RUN pip install -r requirements.txt

# Establecer CRON
COPY ./crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab
RUN ln -sf /proc/1/fd/1 /var/log/cron.log

# Ejecutar cron
CMD /server/entrypoint.sh