#!/bin/sh
# Apply every migration to a throwaway Postgres container and run the
# database checks. Needs Docker; touches no Supabase project.
#
#   sh supabase/tests/run_local.sh        (from the repo root)
#
# Two runs, each in its own database:
#   1. stub + every migration + security_test.sql
#   2. stub + migrations up to Phase 2 + old-style data + the Phase 3
#      migration + phase3_backfill_checks.sql (proves the backfill)
# Exit code is non-zero if any migration or check fails.

set -e
export MSYS_NO_PATHCONV=1                 # Git Bash on Windows: keep container paths as written

ROOT=$(pwd -W 2>/dev/null || pwd)         # Windows path under Git Bash, POSIX path elsewhere
NAME=spendstream-pg-test
PSQL="psql -h 127.0.0.1 -U postgres -v ON_ERROR_STOP=1 -q"

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -e POSTGRES_PASSWORD=postgres \
    -v "$ROOT/supabase:/supabase:ro" postgres:16-alpine >/dev/null
trap 'docker rm -f "$NAME" >/dev/null 2>&1' EXIT

# The image's first-boot server listens only on a socket; wait for TCP.
until docker exec "$NAME" pg_isready -h 127.0.0.1 -U postgres >/dev/null 2>&1; do
    sleep 1
done

# 1. Security checks against the full schema
set -- -f /supabase/tests/local_auth_stub.sql
for f in "$ROOT"/supabase/migrations/*.sql; do
    set -- "$@" -f "/supabase/migrations/$(basename "$f")"
done
set -- "$@" -f /supabase/tests/security_test.sql
docker exec "$NAME" $PSQL "$@"

# 2. Phase 3 backfill of pre-existing data
docker exec "$NAME" $PSQL -c "create database backfill_test"
docker exec "$NAME" $PSQL -d backfill_test \
    -f /supabase/tests/local_auth_stub.sql \
    -f /supabase/migrations/20260915000000_baseline_schema.sql \
    -f /supabase/migrations/20260915000100_phase2_security.sql \
    -f /supabase/tests/phase3_backfill_seed.sql \
    -f /supabase/migrations/20260915000200_phase3_pipeline.sql \
    -f /supabase/tests/phase3_backfill_checks.sql

# 3. A project created before 2026-09-15: hand-made tables, baseline skipped
set -- -f /supabase/tests/local_auth_stub.sql -f /supabase/tests/legacy_schema_seed.sql
for f in "$ROOT"/supabase/migrations/*.sql; do
    case "$(basename "$f")" in *_baseline_schema.sql) continue ;; esac
    set -- "$@" -f "/supabase/migrations/$(basename "$f")"
done
set -- "$@" -f /supabase/tests/legacy_schema_checks.sql
docker exec "$NAME" $PSQL -c "create database legacy_test"
docker exec "$NAME" $PSQL -d legacy_test "$@"
