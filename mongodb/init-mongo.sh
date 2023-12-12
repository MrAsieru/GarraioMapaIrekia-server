#!/bin/bash
mongosh <<EOF
use  ${MONGODB_INITDB_DATABASE}
db.createUser({user: "${MONGODB_SERVER_USER}", pwd: "${MONGODB_SERVER_USER_PASSWORD}", roles: [{role: 'readWrite', db: "${MONGODB_INITDB_DATABASE}"}, {role: 'dbAdmin', db: "${MONGODB_INITDB_DATABASE}"}]})
db.createUser({user: "${MONGODB_API_USER}", pwd: "${MONGODB_API_USER_PASSWORD}", roles: [{role: 'read', db: "${MONGODB_INITDB_DATABASE}"}]})

db.paradas.createIndex({"paradaPadre": 1})
db.posiciones.createIndex({"fecha": 1, "idAgencia": 1})
db.posiciones.createIndex({"idAgencia": 1})
EOF