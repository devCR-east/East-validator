# BFT: seal ~3s when mempool has txs

## Why stake still waited 3 minutes
With `BFT_ENABLED` (default on), **legacy Producer is off**.
Sealing is paced by `BFTEngine.waitBlockInterval()` → `MinBlockInterval` = `BLOCK_INTERVAL_SEC` (180s).
The earlier `producer.go` fast-seal patch never runs.

## Fix
`internal/consensus/bft.go` — if mempool size > 0, pace with **3s** (env `BFT_TX_MIN_INTERVAL_MS`), else full 180s.

## Env
```bash
BLOCK_INTERVAL_SEC=180
BFT_TX_MIN_INTERVAL_MS=3000
BFT_ENABLED=true
```

## Deploy
Overwrite `internal/consensus/bft.go` in east-validator-go → push → Railway redeploy validator.
