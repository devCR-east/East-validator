#!/usr/bin/env bash
# EAST full state sync helper (inspired by Paxi state_sync.sh UX).
# Pulls accounts + recent headers + tip from seed GET /admin/snapshot.
# Usage:
#   export SEED_RPC=https://seed.example.com
#   export API_SECRET=...
#   export DATA_DIR=./data
#   ./scripts/state_sync.sh
set -euo pipefail

SEED_RPC="${SEED_RPC:?set SEED_RPC to seed validator HTTP base URL}"
SEED_RPC="${SEED_RPC%/}"
API_SECRET="${API_SECRET:-}"
DATA_DIR="${DATA_DIR:-./data}"
FORCE="${STATE_SYNC_FORCE:-true}"
OUT="${DATA_DIR}/state-sync-snapshot.json"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: need $1"; exit 1; }; }
need curl
need jq

mkdir -p "$DATA_DIR"

echo "==> Seed status"
curl -fsS "${SEED_RPC}/block/latest" | jq '{height,hash}' || true

echo "==> Download full snapshot (accounts + headers + tip)"
HDRS=()
if [[ -n "$API_SECRET" ]]; then
  HDRS=(-H "X-API-Secret: ${API_SECRET}")
fi
HTTP=$(curl -sS -w "%{http_code}" -o "$OUT" "${HDRS[@]}" \
  "${SEED_RPC}/admin/snapshot?max_blocks=2000")
if [[ "$HTTP" != "200" ]]; then
  echo "ERROR: snapshot HTTP $HTTP (seed must expose GET /admin/snapshot)"
  head -c 400 "$OUT" || true
  exit 1
fi

ACCOUNTS=$(jq '.accounts | length' "$OUT")
HEIGHT=$(jq -r '.latest_height' "$OUT")
echo "OK: tip=$HEIGHT accounts=$ACCOUNTS file=$OUT"
echo ""
echo "Next: start east-validator with:"
echo "  STATE_SYNC_URL=${SEED_RPC}"
echo "  STATE_SYNC_FORCE=${FORCE}"
echo "  API_SECRET=***"
echo "  AUTO_PRODUCE=false"
echo "Or POST the file to a running joiner:"
echo "  curl -X POST -H \"X-API-Secret: \$API_SECRET\" --data-binary @$OUT \\"
echo "    \"http://127.0.0.1:8080/admin/snapshot?force=true\""
