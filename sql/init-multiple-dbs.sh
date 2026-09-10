#!/usr/bin/env bash
# Bootstraps the Postgres instance used by the whole stack:
#   1) a role + database for Airflow's own metadata store
#   2) a role + database for the stock market data the pipeline writes to
# Running both on one Postgres container keeps `docker compose up` to a
# single command while still giving each concern its own db/user.
set -e
set -u

function create_role_and_db() {
    local db=$1
    local role=$2
    local password=$3

    echo "Ensuring role '$role' exists"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
        DO \$\$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$role') THEN
                CREATE ROLE "$role" LOGIN PASSWORD '$password';
            END IF;
        END
        \$\$;
EOSQL

    echo "Ensuring database '$db' exists and is owned by '$role'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
        SELECT 'CREATE DATABASE "$db" OWNER "$role"'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL

    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
        GRANT ALL PRIVILEGES ON DATABASE "$db" TO "$role";
EOSQL
}

create_role_and_db "${AIRFLOW_DB_NAME}" "${AIRFLOW_DB_USER}" "${AIRFLOW_DB_PASSWORD}"
create_role_and_db "${STOCK_DB_NAME}" "${STOCK_DB_USER}" "${STOCK_DB_PASSWORD}"

echo "Database bootstrap complete."
