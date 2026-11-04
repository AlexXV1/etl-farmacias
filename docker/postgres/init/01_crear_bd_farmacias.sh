#!/bin/bash
# Se ejecuta UNA sola vez: cuando PostgreSQL inicializa un volumen vacío.
# Crea el rol de aplicación (sin superusuario) y la base 'farmacias',
# separada de la base 'airflow' que guarda los metadatos del orquestador.
# Las credenciales llegan por variables de entorno desde .env: nunca se escriben aquí.
set -euo pipefail

# -v pasa las variables a psql; :"x" las cita como identificador y :'x' como texto,
# así un valor raro no puede romper ni inyectar SQL.
psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v app_user="$FARMACIAS_DB_USER" -v app_pass="$FARMACIAS_DB_PASSWORD" -v app_db="$FARMACIAS_DB_NAME" <<-'EOSQL'
    CREATE ROLE :"app_user" LOGIN PASSWORD :'app_pass';
    CREATE DATABASE :"app_db" OWNER :"app_user";
    REVOKE ALL ON DATABASE :"app_db" FROM PUBLIC;
EOSQL
