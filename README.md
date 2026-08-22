# EAST Validator — combined update

English docs. This package merges:

1. **Cosmos-style P2P addresses** (`id@host:26656`)
2. **Full state sync** (accounts + tip via `GET /admin/snapshot`)

Wire protocol remains **libp2p** (not CometBFT PEX). State sync uses **HTTP snapshot** (not height-only archive).

---

## 1. P2P — Cosmos-style peers

```bash
P2P_ENABLED=true
P2P_PORT=26656
P2P_BOOTSTRAP=12D3KooWSEED...@seed.example.com:26656
P2P_ANNOUNCE_ADDR=12D3KooWLOCAL...@vps-or-proxy.example.com:26656
```

Multiaddrs still work: `/dns4/host/tcp/26656/p2p/12D3KooW...`

`id` must be an EAST libp2p peer id (`12D3KooW…`), not a foreign CometBFT hex id.

---

## 2. Full state sync (not height-only)

### Seed (node 1)

Deploy this build. Check:

```bash
curl -sS -H "X-API-Secret: $API_SECRET" \
  "$SEED/admin/snapshot?max_blocks=10" | jq '{latest_height, n:(.accounts|length)}'
```

Must be **HTTP 200**.

### Joiner (VPS / node 2 / node 3)

```bash
STATE_SYNC_URL=https://seed.example.com
STATE_SYNC_FORCE=true
API_SECRET=same-as-seed
AUTO_PRODUCE=false
P2P_BOOTSTRAP=12D3KooWSEED...@seed.example.com:26656
# + CHAIN_SIGNING_PRIVATE_KEY, GENESIS_PATH, DATA_DIR
```

On boot: download snapshot → import **balances, stake, nonce, recent headers, tip**.

Helper script:

```bash
export SEED_RPC=https://seed.example.com
export API_SECRET=...
./scripts/state_sync.sh
```

### API

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/admin/snapshot?max_blocks=2000` | Export full state JSON |
| POST | `/admin/snapshot?force=true` | Import snapshot body |

---

## 3. Recommended joiner env (all-in-one)

```bash
CHAIN_SIGNING_PRIVATE_KEY=0x...
DATA_DIR=/var/lib/east
GENESIS_PATH=/var/lib/east/genesis.json
HTTP_ADDR=:8080
API_SECRET=...

P2P_ENABLED=true
P2P_PORT=26656
P2P_BOOTSTRAP=12D3KooWSEED...@seed-host:26656
P2P_ANNOUNCE_ADDR=12D3KooWLOCAL...@VPS_IP:26656

STATE_SYNC_URL=https://seed-http.example.com
STATE_SYNC_FORCE=true
AUTO_PRODUCE=false
```

Open firewall TCP `26656`. Stake ≥ network minimum before consensus produce.

---

## 4. Files touched

| Path | Change |
|------|--------|
| `internal/p2p/peeraddr.go` | Parse `id@host:port` |
| `internal/p2p/node.go` | Bootstrap/announce + default port 26656 |
| `internal/state/backup.go` | Export/import full snapshot + `SyncFromURL` |
| `internal/api/server.go` | `GET/POST /admin/snapshot` |
| `cmd/validator/main.go` | Boot `STATE_SYNC_URL` |
| `scripts/state_sync.sh` | Operator helper |
| `README.md` | This document |

---

## 5. Apply

```bash
cd east-validator
unzip -o east-validator-p2p-and-state-sync.zip
# merge paths, commit, deploy seed first, then joiners
```

Deploy **seed before** joiners so `/admin/snapshot` is not 404.
