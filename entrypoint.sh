printenv > /etc/environment

bash /server/scripts/iniciar.sh > /var/log/cron.log 2>/var/log/cron.log

cron -L 15 -f