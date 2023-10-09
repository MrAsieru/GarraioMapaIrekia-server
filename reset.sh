docker compose down
docker image prune -a -f
docker volume prune -a -f
docker network prune -f