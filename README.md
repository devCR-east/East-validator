# Validator: fast mempool seal + archive/P2P header catch-up

Combined package (two features, one deploy tree).

## 1) Fast seal (`internal/consensus/producer.go`)
- Empty mempool → seal on `BLOCK_INTERVAL_SEC` (e.g. 180s)
- Mempool has txs → try seal every ~3s (`TX_SEAL_POLL_MS`)
- Min gap ~2s (`MIN_SEAL_GAP_MS`)
- Leader / external proposal checks unchanged

## 2) Archive + P2P catch-up (team)
- `internal/sync/archive.go` — headers from Vercel/Neon in chunks of 500
- `cmd/validator/main.go` — startup: archive then P2P for remainder
- `internal/config/config.go` — `ARCHIVE_SYNC_URL`

**Headers only** — does not restore balances/stake.

## Merge into `east-validator-go`
```text
internal/consensus/producer.go   ← overwrite
internal/sync/archive.go         ← add/overwrite
internal/config/config.go        ← merge carefully (keep your extra fields)
cmd/validator/main.go            ← merge carefully (archive startup + your BFT/P2P wires)
```

## Env
```bash
BLOCK_INTERVAL_SEC=180
TX_SEAL_POLL_MS=3000
MIN_SEAL_GAP_MS=2000
ARCHIVE_SYNC_URL=https://thiseast.vercel.app
AUTO_PRODUCE=true
```

Node 2: different CHAIN_SIGNING_*, DATA_DIR, NODE_ID, P2P_*; shared VALIDATORS list.
