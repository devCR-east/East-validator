# BFT fast seal when mempool has txs (v2 — poll while waiting)

## Root cause of "still 3 minutes"
1. BFT is ON → Producer fast-seal never runs
2. `waitBlockInterval` decided **once** at start of wait with **empty** mempool → sleep 180s
3. Stake/send tx arrives **during** that sleep → still waited full 180s
4. `soloSeal` called `waitBlockInterval` **again** (double pace)

## Fix
- While pacing, **re-check mempool every 500ms**
- If Size()>0 → only need **3s** since last commit (env `BFT_TX_MIN_INTERVAL_MS`)
- If empty → still need full `BLOCK_INTERVAL_SEC` (180s)
- Remove extra wait inside `soloSeal`

## Deploy
Overwrite `internal/consensus/bft.go` → push Railway → **must redeploy**

```bash
BFT_TX_MIN_INTERVAL_MS=3000
BLOCK_INTERVAL_SEC=180
```

After deploy, log should show near stake time:
`BFT: mempool non-empty — releasing pace early`
or seal within ~3s of submitting stake.
