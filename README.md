# east-validator (Go) — Sealer + Local State

Lightweight validator for EASTCHAIN, designed for the **Railway free tier**.

## Role

- **Fullnode Browser** (online, stake ≥ threshold) → proposes blocks in turn
- **This service (Railway)** → verifies signatures, seals blocks, persists state
- **Lightnode** → receives sealed block headers

## Features (Phase 1.2)

- **BFT consensus** (Tendermint-inspired, default on): Propose → Prevote → Precommit → Commit with **+2/3 quorum**
- **Mempool** (CometBFT-style): CheckTx → queue → included on seal
- **Leader election**: solo always produces; **2+ validators** rotate by `height % n`
- **Auto block producer**: legacy path when `BFT_ENABLED=false`; with BFT the engine drives sealing
- Numeric EIP-155 chain ID **172026** + string id `eastchain-1`
- **libp2p GossipSub** — blocks, heartbeats, **and consensus** (proposals/votes/commits)
- Local state (BadgerDB): balances, stake, pending unstake, nonces, uptime scores
- **Genesis** with hard cap **1,000,000,000 EAST** + supply buckets (aligned with existing tokenomics)
- **EIP-191 / secp256k1** signatures (compatible with ethers `personal_sign`)
- `POST /consensus/propose` for Fullnode Browser proposals
- `GET /consensus/status` — live BFT height/round/step/quorum
- Old-block pruning (default keep 3000)
- Single binary, small Dockerfile

### BFT consensus (Phase 1.2)

Enabled by default (`BFT_ENABLED` unset or anything except `false`).

| Validators | Behaviour |
|------------|-----------|
| 0–1 | Solo seal (no voting) — same as before |
| 2+ | Full round: leader proposes → all prevote → +2/3 → precommit → +2/3 → **commit** |

Vote / proposal messages are EIP-191 signed:

```
EASTCHAIN_VOTE|{prevote|precommit}|{height}|{round}|{blockHash|NIL}
EASTCHAIN_BFT_PROPOSAL|{height}|{round}|{blockHash}|{prevHash}
```

P2P topic: `eastchain/consensus/1.0.0`

Double-sign (equivocation) is detected and logged; jailing is a later phase.

Set `BFT_ENABLED=false` to fall back to the legacy interval-based `Producer`.

## Environment variables (Railway)

### Required (production)

| Variable | Required | Description |
|----------|----------|-------------|
| `API_SECRET` | **yes** | Shared secret for write endpoints (`X-API-Secret`). Reject all writes if empty in production. |
| `CHAIN_SIGNING_PRIVATE_KEY` | **yes** | `0x` + 32-byte hex secp256k1 key used to seal blocks and sign BFT votes/proposals. |
| `CHAIN_SIGNING_ADDRESS` | **yes** | EVM address of `CHAIN_SIGNING_PRIVATE_KEY`. Used for leader election and proposal identity. |
| `DATA_DIR` | **yes** | Persistent state path (BadgerDB). On Railway: mount a **Volume** at `/app/data`. Default `./data`. |
| `P2P_PRIVATE_KEY` | **yes** (prod) | Stable libp2p identity (protobuf-encoded private key, hex). If unset, peer ID changes on every restart and breaks bootstrap lists. |

### Required for specific features

| Variable | Required when | Description |
|----------|---------------|-------------|
| `MINING_ORACLE_ADDRESS` | using `claim_mining` | EVM address allowed to sign `EASTCHAIN_MINT\|...` oracle messages. Claim mining is **disabled** if unset. |
| `VALIDATORS` | multi-validator / BFT | Comma-separated EVM addresses of the active validator set. Enables round-robin leader election and BFT quorum when 2+. |
| `P2P_BOOTSTRAP` | joining an existing network | Comma-separated multiaddrs of seed peers, e.g. `/ip4/x.x.x.x/tcp/4001/p2p/12D3KooW...`. First node may leave this empty. |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `validator-1` | Human-readable node label in logs/stats. |
| `NODE_NAME` | `east-validator` | Display name in `/health`. |
| `GENESIS_PATH` | `/app/genesis.json` | Path to genesis JSON (supply buckets, chain id). |
| `KEEP_RECENT_BLOCKS` | `3000` | Local block history retention before prune. |
| `BLOCK_INTERVAL_SEC` | `120` | Target block interval (solo/legacy producer; BFT uses round timeouts). |
| `AUTO_PRODUCE` | `true` | Legacy interval producer when `BFT_ENABLED=false`. |
| `BFT_ENABLED` | `true` | Tendermint-style BFT (Propose → Prevote → Precommit → Commit). Set `false` for legacy producer only. |
| `EPOCH_SECONDS` | `604800` | Uptime epoch length (1 week). |
| `P2P_ENABLED` | `true` | Enable libp2p (GossipSub, DHT, mDNS, block sync). |
| `P2P_PORT` | `4001` | TCP listen port for libp2p. Must be reachable by peers (open firewall / Railway TCP). |
| `P2P_CONN_LOW` | `50` | Connection-manager low water mark (do not prune below this). |
| `P2P_CONN_HIGH` | `200` | Connection-manager high water mark (prune above this). |
| `HTTP_ADDR` | `:$PORT` or `:8080` | HTTP API bind address. |
| `PORT` | (Railway) | Injected by Railway; used when `HTTP_ADDR` is unset. |

### Minimal production example

```bash
API_SECRET=<long-random-string>
CHAIN_SIGNING_PRIVATE_KEY=0x...
CHAIN_SIGNING_ADDRESS=0xYourValidatorAddress
DATA_DIR=/app/data
P2P_PRIVATE_KEY=<stable-libp2p-protobuf-hex>
P2P_PORT=4001
P2P_BOOTSTRAP=/ip4/<seed-ip>/tcp/4001/p2p/<seed-peer-id>
VALIDATORS=0xValidatorA,0xValidatorB,0xValidatorC
MINING_ORACLE_ADDRESS=0xOracleAddress   # if mining claims are enabled
BFT_ENABLED=true
```

## API

### Public
```
GET /health
GET /stats
GET /account/{address}
GET /block/latest
GET /block/{height}
GET /supply
GET /supply/{bucket}
GET /p2p
```

### Uptime / node integrity
```
POST /heartbeat          { "address", "node_id", "tier": "light"|"full" }
GET  /uptime/{address}
GET  /uptime/epoch/{epoch}?limit=100
```
Default epoch = 7 days (`EPOCH_SECONDS=604800`). Heartbeat count per epoch is the Phase-1 uptime score.

### Protected (`X-API-Secret`)
```
POST /tx
POST /consensus/propose
POST /admin/seed
POST /admin/prune
POST /admin/validators   { "validators": ["0x...", "0x..."] }
```

### Example propose (Fullnode Browser)

```json
POST /consensus/propose
{
  "proposal_id": "prop-123",
  "height": 1,
  "prev_hash": "GENESIS",
  "tx_hashes": ["0xabc..."],
  "merkle_root": "0x...",
  "block_hash": "0x...",
  "timestamp": 1730000000000,
  "proposer": "0xYourEvmAddress",
  "signature": "0x...EIP191..."
}
```

Proposal signature message format:
```
EASTCHAIN_PROPOSAL|{proposal_id}|{height}|{block_hash}
```

Sealer signs the header with:
```
EASTCHAIN_BLOCK|{height}|{block_hash}
```

## Genesis / Max Supply

Hard-coded and validated at boot:

| Bucket | Cap |
|--------|-----|
| mining | 300,000,000 |
| staking | 80,000,000 |
| validator | 80,000,000 |
| campaign | 40,000,000 |
| liquidity | 150,000,000 |
| treasury | 100,000,000 |
| emergency | 70,000,000 |
| marketing | 70,000,000 |
| team | 60,000,000 |
| founder | 50,000,000 |
| **TOTAL** | **1,000,000,000** |

Min validator stake: **100 EAST**  
Min fullnode stake: **10 EAST**

## Deploy on Railway

1. Push the repo to GitHub
2. New Project → Deploy from GitHub
3. **Volume** → mount `/app/data`
4. Set the environment variables above
5. Healthcheck path: `/health`
6. Open TCP **4001** for libp2p (private networking between services is preferred)

## P2P (libp2p)

### Peer discovery (public network)

Nodes discover each other via:

1. **Bootstrap seeds** — `P2P_BOOTSTRAP` multiaddrs (first contact)
2. **Kademlia DHT** — every validator runs `ModeServer` and advertises under namespace `eastchain-validators-v1`
3. **mDNS** — automatic on the same LAN / docker network (`eastchain._udp`)
4. **Hole punching + AutoRelay + UPnP** — improves reachability behind NAT

After the first few seeds are online, additional validators can join with an empty bootstrap list once they learn at least one peer (or share one seed).

Connection manager defaults: keep ~50–200 peers (`P2P_CONN_LOW` / `P2P_CONN_HIGH`) to resist connection floods.

Block catch-up: stream protocol `/eastchain/blocksync/1.0.0` (request up to 50 headers per call).

Topics:
Topics:
- `eastchain/blocks/1.0.0` — block header announce after seal
- `eastchain/heartbeats/1.0.0` — uptime gossip

After deploy, call `GET /p2p` and copy a value from `listen`, then set that multiaddr as `P2P_BOOTSTRAP` on other peers.

## Still TODO (do not use real funds yet)

- Leader schedule persistence + gossip of validator set
- Real Merkle state root
- Full block sync for peers that fall behind
- DHT / mDNS discovery beyond manual bootstrap
- claim_staking / campaign mint paths (only claim_mining is wired)
- Unit unification: bucket caps are human EAST; balances are 6-decimal subunits

## claim_mining (Phase 1 — on-chain mint from `mining` bucket)

Replaces Neon `mintFromBucket('mining')` for the mining path.

1. Set `MINING_ORACLE_ADDRESS` to the oracle EVM address (Vercel/service key).
2. Oracle signs (ethers `personal_sign`):
   ```
   EASTCHAIN_MINT|mining|{beneficiary}|{amount}|{nonce}|{epoch_id}
   ```
   `amount` = human EAST (1 = 1 EAST), same unit as genesis bucket caps.
3. Beneficiary signs the tx hash as usual (`EASTCHAIN_TX|{hash}`).
4. `POST /tx` body example:
   ```json
   {
     "type": "claim_mining",
     "from": "0xBeneficiary...",
     "amount": 10,
     "nonce": 1,
     "timestamp": 1730000000000,
     "signature": "0x...user...",
     "payload": {
       "bucket": "mining",
       "epoch_id": 42,
       "oracle_signature": "0x...oracle..."
     }
   }
   ```
5. On seal: bucket.minted += amount; balance += amount * 1_000_000 (6 decimals).


## Mempool & leader election

### Mempool
`POST /tx` runs **CheckTx** (nonce, balance, basic rules) and queues the transaction.
On each auto-seal (when this node is leader), up to 100 txs are taken from the pool, applied, and committed in the block.

### Leader election (CometBFT-inspired)
| Validator count | Behavior |
|-----------------|----------|
| 0–1 | Local node always produces (solo / single) |
| 2+ | Round-robin: leader for height `h` is `validators[h % n]` |

Set the set via env:
```
VALIDATORS=0xaaa...,0xbbb...,0xccc...
```
Or at runtime (protected):
```
POST /admin/validators
{ "validators": ["0xaaa...", "0xbbb..."] }
```

Each node must set `CHAIN_SIGNING_ADDRESS` to its own address so `IsLocalLeader` works.
Inspect schedule: `GET /consensus/leader`.
