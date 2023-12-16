printenv > /etc/environment

cron -L 15 -f &

bash /server/scripts/iniciar.sh > /var/log/cron.log 2>/var/log/cron.log