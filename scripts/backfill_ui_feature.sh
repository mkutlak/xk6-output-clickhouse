#!/usr/bin/env bash
# backfill_ui_feature.sh
#
# Backfills the ui_feature column in k6.samples from extra_tags['uiFeature']
# for rows where ui_feature is empty but the camelCase tag was stored instead.
#
# ClickHouse mutations are asynchronous. This script fires the mutation and
# polls system.mutations until it completes.
#
# Usage:
#   ./scripts/backfill_ui_feature.sh [options]
#
# Options:
#   -h HOST      ClickHouse host (default: localhost)
#   -p PORT      ClickHouse port (default: 9000)
#   -u USER      ClickHouse user (default: default)
#   -P PASSWORD  ClickHouse password (default: "")
#   -d DATABASE  ClickHouse database (default: k6)
#   -t TABLE     ClickHouse table (default: samples)
#   --dry-run    Show affected row count and SQL without executing
#   --no-wait    Fire mutation and exit immediately (don't poll for completion)

set -euo pipefail

# Defaults
CH_HOST="localhost"
CH_PORT="9000"
CH_USER="default"
CH_PASSWORD=""
CH_DATABASE="k6"
CH_TABLE="samples"
DRY_RUN=false
NO_WAIT=false
POLL_INTERVAL=5  # seconds between progress checks

usage() {
    sed -n '/^# Usage:/,/^[^#]/p' "$0" | sed 's/^# \?//' | head -n -1
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h) CH_HOST="$2"; shift 2 ;;
        -p) CH_PORT="$2"; shift 2 ;;
        -u) CH_USER="$2"; shift 2 ;;
        -P) CH_PASSWORD="$2"; shift 2 ;;
        -d) CH_DATABASE="$2"; shift 2 ;;
        -t) CH_TABLE="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        --no-wait) NO_WAIT=true; shift ;;
        --help) usage ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

ch_query() {
    clickhouse-client \
        --host "$CH_HOST" \
        --port "$CH_PORT" \
        --user "$CH_USER" \
        --password "$CH_PASSWORD" \
        --query "$1"
}

FULL_TABLE="${CH_DATABASE}.${CH_TABLE}"

echo "==> Connecting to ClickHouse at ${CH_HOST}:${CH_PORT} (db: ${CH_DATABASE})"

# Count rows that need backfilling
echo "==> Counting rows with empty ui_feature but uiFeature in extra_tags..."
AFFECTED=$(ch_query "
    SELECT count()
    FROM ${FULL_TABLE}
    WHERE ui_feature = '' AND extra_tags['uiFeature'] != ''
")
echo "    Affected rows: ${AFFECTED}"

if [[ "$AFFECTED" -eq 0 ]]; then
    echo "==> Nothing to backfill. Exiting."
    exit 0
fi

# The mutation SQL:
#   1. Set ui_feature from extra_tags['uiFeature']
#   2. Remove the uiFeature key from extra_tags to avoid duplication
MUTATION_SQL="
ALTER TABLE ${FULL_TABLE}
    UPDATE
        ui_feature  = extra_tags['uiFeature'],
        extra_tags  = mapFilter((k, v) -> k != 'uiFeature', extra_tags)
    WHERE ui_feature = '' AND extra_tags['uiFeature'] != ''
"

if [[ "$DRY_RUN" == true ]]; then
    echo "==> DRY RUN — SQL that would be executed:"
    echo "$MUTATION_SQL"
    exit 0
fi

echo "==> Submitting mutation..."
ch_query "$MUTATION_SQL"

echo "==> Mutation submitted."

if [[ "$NO_WAIT" == true ]]; then
    echo "==> --no-wait set. Check progress with:"
    echo "    SELECT mutation_id, command, parts_to_do, is_done, latest_fail_reason"
    echo "    FROM system.mutations"
    echo "    WHERE database = '${CH_DATABASE}' AND table = '${CH_TABLE}'"
    echo "    ORDER BY create_time DESC LIMIT 5"
    exit 0
fi

# Poll system.mutations until the latest mutation on this table is done
echo "==> Waiting for mutation to complete (polling every ${POLL_INTERVAL}s)..."
echo "    Press Ctrl-C to stop waiting; the mutation will continue in the background."
echo ""

while true; do
    STATUS=$(ch_query "
        SELECT
            mutation_id,
            parts_to_do,
            is_done,
            latest_fail_reason
        FROM system.mutations
        WHERE database = '${CH_DATABASE}'
          AND table = '${CH_TABLE}'
          AND command LIKE '%uiFeature%'
        ORDER BY create_time DESC
        LIMIT 1
        FORMAT TabSeparated
    ")

    if [[ -z "$STATUS" ]]; then
        echo "    No matching mutation found yet, retrying..."
        sleep "$POLL_INTERVAL"
        continue
    fi

    MUTATION_ID=$(echo "$STATUS" | cut -f1)
    PARTS_TO_DO=$(echo "$STATUS" | cut -f2)
    IS_DONE=$(echo "$STATUS"    | cut -f3)
    FAIL_REASON=$(echo "$STATUS" | cut -f4)

    TIMESTAMP=$(date '+%H:%M:%S')

    if [[ "$IS_DONE" == "1" ]]; then
        echo "    [${TIMESTAMP}] Mutation ${MUTATION_ID} completed."
        break
    fi

    if [[ -n "$FAIL_REASON" && "$FAIL_REASON" != "0" ]]; then
        echo "    [${TIMESTAMP}] Mutation ${MUTATION_ID} failed: ${FAIL_REASON}" >&2
        exit 1
    fi

    echo "    [${TIMESTAMP}] Mutation ${MUTATION_ID} in progress — parts remaining: ${PARTS_TO_DO}"
    sleep "$POLL_INTERVAL"
done

# Verify
REMAINING=$(ch_query "
    SELECT count()
    FROM ${FULL_TABLE}
    WHERE ui_feature = '' AND extra_tags['uiFeature'] != ''
")
echo ""
echo "==> Backfill complete."
echo "    Rows backfilled : ${AFFECTED}"
echo "    Rows still unset: ${REMAINING}"

if [[ "$REMAINING" -gt 0 ]]; then
    echo "    WARNING: ${REMAINING} rows still have empty ui_feature. Check system.mutations for errors." >&2
fi
