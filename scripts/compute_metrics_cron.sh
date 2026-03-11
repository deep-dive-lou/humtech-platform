#!/bin/bash
# Compute weekly metric snapshots for all active tenants.
# Designed for cron: 0 6 * * 1 /opt/humtech/app/scripts/compute_metrics_cron.sh
#
# Runs inside the Docker container via docker exec.

set -euo pipefail

LOG_FILE="/var/log/metric-snapshots.log"
CONTAINER="app-humtech_runner-1"

echo "$(date -u '+%Y-%m-%d %H:%M:%S') Starting metric snapshot computation" >> "$LOG_FILE"

# Get all active tenant slugs
SLUGS=$(docker exec "$CONTAINER" python -c "
import asyncio, os, asyncpg
async def get_slugs():
    conn = await asyncpg.connect(os.environ['DATABASE_URL'])
    rows = await conn.fetch('SELECT slug FROM core.tenants WHERE is_active = TRUE')
    await conn.close()
    for r in rows:
        print(r[\"slug\"])
asyncio.run(get_slugs())
" 2>>"$LOG_FILE")

for SLUG in $SLUGS; do
    echo "$(date -u '+%Y-%m-%d %H:%M:%S') Computing snapshot for tenant: $SLUG" >> "$LOG_FILE"
    docker exec -e TENANT_SLUG="$SLUG" "$CONTAINER" \
        python scripts/compute_metric_snapshot.py >> "$LOG_FILE" 2>&1 || \
        echo "$(date -u '+%Y-%m-%d %H:%M:%S') ERROR: Failed for $SLUG" >> "$LOG_FILE"
done

echo "$(date -u '+%Y-%m-%d %H:%M:%S') Metric snapshot computation complete" >> "$LOG_FILE"
