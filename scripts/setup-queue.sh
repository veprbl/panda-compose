#!/usr/bin/env bash
# setup-queue.sh — register the PANDA_COMPOSE_LOCAL compute queue directly in the
# PanDA PostgreSQL database.  Called by the 'init' service after postgres is healthy.
# Runs as the postgres superuser so it can create the atlas_panda schema alias.
set -euo pipefail

PGHOST="${PGHOST:-postgres}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres_secret}"
PGDATABASE="${PGDATABASE:-panda_db}"
PANDA_DB_USER="${PANDA_DB_USER:-panda}"
QUEUE_NAME="PANDA_COMPOSE_LOCAL"

export PGPASSWORD

psql() { command psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" "$@"; }

SQLS_DIR=/docker-entrypoint-initdb.d/sqls
NEED_FINISH=$(psql -tAqc "SELECT NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'doma_pandameta')")
if [ "$NEED_FINISH" = "t" ]; then
    echo "Finishing interrupted panda-database schema init..."

    echo "  creating missing doma_panda.worker_node_queue..."
    psql -v ON_ERROR_STOP=1 <<'ENDOFSQL'
CREATE TABLE IF NOT EXISTS doma_panda.worker_node_queue (
    site         VARCHAR(64)  NOT NULL,
    host_name    VARCHAR(255) NOT NULL,
    panda_queue  VARCHAR(64)  NOT NULL,
    last_seen    TIMESTAMP
);
ALTER TABLE doma_panda.worker_node_queue OWNER TO panda;
ENDOFSQL

    for f in \
        pg_PANDA_SCHEDULER_JOBS.sql \
        pg_PANDAMETA_TABLE.sql \
        pg_PANDAMETA_SEQUENCE.sql \
        pg_PANDAARCH_TABLE.sql \
        pg_PANDABIGMON_TABLE.sql \
        pg_PANDABIGMON_SEQUENCE.sql \
        pg_PANDABIGMON_VIEW.sql \
        pg_PANDABIGMON_TRIGGER.sql \
        pg_PANDABIGMON_PROCEDURE.sql \
        pg_DEFT_TABLE.sql \
        pg_PARTITION.sql
    do
        echo "  applying ${f}..."
        psql -tAqc "SELECT pg_read_file('${SQLS_DIR}/${f}')" | psql
    done
    echo "Finished panda-database schema init."
else
    echo "panda-database schema already complete; skipping finish step."
fi

# Step 1: create atlas_panda schema as an alias for doma_panda.
# Some PanDA server code paths hard-code "ATLAS_PANDA" table references (Oracle legacy);
# simple updatable views let those queries work against the PostgreSQL doma_panda schema.
echo "Creating atlas_panda schema alias..."
psql -v ON_ERROR_STOP=1 -c "CREATE SCHEMA IF NOT EXISTS atlas_panda;"
psql -v ON_ERROR_STOP=1 -c "GRANT USAGE ON SCHEMA atlas_panda TO ${PANDA_DB_USER};"

psql -v ON_ERROR_STOP=1 << 'ENDOFSQL'
DO $$
DECLARE
    t RECORD;
    cnt INT := 0;
BEGIN
    FOR t IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'doma_panda'
          AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format(
            'CREATE OR REPLACE VIEW atlas_panda.%I AS SELECT * FROM doma_panda.%I',
            t.table_name, t.table_name
        );
        cnt := cnt + 1;
    END LOOP;
    RAISE NOTICE 'Created % views in atlas_panda', cnt;
END$$;
ENDOFSQL

psql -v ON_ERROR_STOP=1 -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA atlas_panda TO ${PANDA_DB_USER};"
echo "atlas_panda schema alias created."

# Step 1b: insert the JEDI schema version row so SchemaChecker.py succeeds on a fresh DB.
# The checker queries: SELECT major||'.'||minor||'.'||patch FROM ATLAS_PANDA.pandadb_version
# WHERE component = 'JEDI'.  Without this row it raises IndexError and JEDI never starts.
echo "Inserting JEDI schema version row..."
psql -v ON_ERROR_STOP=1 -c \
  "INSERT INTO doma_panda.pandadb_version (component, major, minor, patch)
   VALUES ('JEDI', 0, 0, 24)
   ON CONFLICT (component) DO NOTHING;"
echo "JEDI schema version row inserted."

# Step 1c: seed resource_types with permissive SCORE entry so tasks requesting
# 2000 MB/core can match. Without this, tasks stay resource_type='Undefined'.
echo "Seeding resource_types..."
psql -v ON_ERROR_STOP=1 << 'ENDOFSQL'
INSERT INTO doma_panda.resource_types (resource_name, minrampercore, maxrampercore, maxtime, corecount)
VALUES ('SCORE', 0, 8192, 86400, 1)
ON CONFLICT (resource_name) DO UPDATE
  SET minrampercore = 0, maxrampercore = 8192;
ENDOFSQL
echo "resource_types seeded."

# Step 2: register PANDA_COMPOSE_LOCAL queue using the panda user credentials.
export PGPASSWORD="${PANDA_DB_PASSWORD:-panda_secret}"
export PGUSER="${PANDA_DB_USER}"

echo "Registering queue '${QUEUE_NAME}' in postgres at ${PGHOST}:${PGPORT}..."

psql -v ON_ERROR_STOP=1 << 'ENDOFSQL'
-- panda_site: map the queue to itself
INSERT INTO doma_panda.panda_site (panda_site_name, site_name, is_local)
VALUES ('PANDA_COMPOSE_LOCAL', 'PANDA_COMPOSE_LOCAL', 'Y')
ON CONFLICT (panda_site_name) DO NOTHING;

-- schedconfig_json: full queue spec consumed by JEDI and Harvester
INSERT INTO doma_panda.schedconfig_json (panda_queue, data, last_update)
VALUES (
  'PANDA_COMPOSE_LOCAL',
  '{
    "panda_queue":        "PANDA_COMPOSE_LOCAL",
    "nickname":           "PANDA_COMPOSE_LOCAL",
    "panda_resource":     "PANDA_COMPOSE_LOCAL",
    "site_name":          "PANDA_COMPOSE_LOCAL",
    "status":             "online",
    "queue_type":         "unified",
    "type":               "analysis",
    "cloud":              "LOCAL",
    "nqueue":             10,
    "maxtime":            3600,
    "maxmemory":          2000,
    "corecount":          1,
    "maxcpucount":        1,
    "mintime":            0,
    "maxinputsize":       0,
    "vo":                 "wlcg",
    "harvester":          "panda-compose-harvester",
    "harvester_id":       "panda-compose-harvester",
    "use_newmover":       "Y",
    "catchall":           "singularity=false",
    "pilot_version":      "latest",
    "job_type":           "unified",
    "direct_access_lan":  false,
    "direct_access_wan":  false
  }',
  NOW()
)
ON CONFLICT (panda_queue) DO UPDATE
  SET data = EXCLUDED.data, last_update = NOW();
ENDOFSQL

echo "Queue '${QUEUE_NAME}' registered. Done."
