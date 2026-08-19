package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/genesis"
	"github.com/eastchain/east-validator/internal/tx"
)

type Account struct {
	Balance        int64  `json:"balance"`
	Staked         int64  `json:"staked"`
	PendingUnstake int64  `json:"pending_unstake"`
	Nonce          uint64 `json:"nonce"`
}

type BlockHeader struct {
	Height    uint64   `json:"height"`
	Hash      string   `json:"hash"`
	PrevHash  string   `json:"prev_hash"`
	StateRoot string   `json:"state_root"`
	TxHashes  []string `json:"tx_hashes"`
	Timestamp int64    `json:"timestamp"`
	Proposer  string   `json:"proposer"`
	TxCount   int      `json:"tx_count"`
	Signature string   `json:"signature,omitempty"`
	// LastCommitRound / LastCommitVotes: finality certificate (precommit count at commit).
	LastCommitRound int32  `json:"last_commit_round,omitempty"`
	LastCommitVotes int    `json:"last_commit_votes,omitempty"`
	LastCommitHash  string `json:"last_commit_hash,omitempty"` // hash of concatenated vote sigs (optional)
}

type Store struct {
	db   *badger.DB
	mu   sync.RWMutex
	path string

	totalMaxSupply    int64
	minValidatorStake int64
	chainSigningAddr  string
	numericChainID    int64
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(filepath.Join(dataDir, "badger"))
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	s := &Store{db: db, path: dataDir}
	log.Info().Str("path", dataDir).Msg("local state opened")
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) InitGenesis(g *genesis.Genesis) error {
	if err := g.Validate(); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(metaKey("genesis_hash")); err == nil {
			log.Info().Msg("genesis already applied — skipping")
			s.totalMaxSupply = g.TotalMaxSupply
			s.minValidatorStake = g.MinValidatorStake
			s.chainSigningAddr = g.ChainSigningAddress
			s.numericChainID = g.NumericChainID
			return nil
		}

		for _, b := range g.Buckets {
			val, _ := json.Marshal(b)
			if err := txn.Set(bucketKey(b.Name), val); err != nil {
				return err
			}
		}
		for _, a := range g.Accounts {
			acc := Account{Balance: a.Balance, Staked: a.Staked}
			val, _ := json.Marshal(acc)
			if err := txn.Set(accountKey(a.Address), val); err != nil {
				return err
			}
		}

		meta, _ := json.Marshal(map[string]any{
			"chain_id":              g.ChainID,
			"numeric_chain_id":      g.NumericChainID,
			"genesis_time":          g.GenesisTime,
			"total_max_supply":      g.TotalMaxSupply,
			"min_validator_stake":   g.MinValidatorStake,
			"chain_signing_address": g.ChainSigningAddress,
		})
		if err := txn.Set(metaKey("genesis"), meta); err != nil {
			return err
		}
		if err := txn.Set(metaKey("genesis_hash"), []byte("applied")); err != nil {
			return err
		}
		if err := txn.Set(metaKey("latest_height"), []byte("0")); err != nil {
			return err
		}

		s.totalMaxSupply = g.TotalMaxSupply
		s.minValidatorStake = g.MinValidatorStake
		s.chainSigningAddr = g.ChainSigningAddress
		s.numericChainID = g.NumericChainID
		log.Info().
			Int64("max_supply", g.TotalMaxSupply).
			Int("buckets", len(g.Buckets)).
			Int("accounts", len(g.Accounts)).
			Msg("genesis applied")
		return nil
	})
}

func (s *Store) TotalMaxSupply() int64       { return s.totalMaxSupply }
func (s *Store) MinValidatorStake() int64    { return s.minValidatorStake }
func (s *Store) ChainSigningAddress() string { return s.chainSigningAddr }
func (s *Store) NumericChainID() int64       { return s.numericChainID }

func normalizeAddr(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

func accountKey(addr string) []byte { return []byte("acc:" + normalizeAddr(addr)) }
func bucketKey(name string) []byte  { return []byte("bucket:" + name) }
func blockKey(height uint64) []byte { return []byte(fmt.Sprintf("blk:%020d", height)) }
func metaKey(key string) []byte     { return []byte("meta:" + key) }

func (s *Store) GetAccount(addr string) (Account, error) {
	var acc Account
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(accountKey(addr))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { return json.Unmarshal(val, &acc) })
	})
	return acc, err
}

func getAccountTxn(txn *badger.Txn, addr string) (Account, error) {
	var acc Account
	norm := normalizeAddr(addr)
	item, err := txn.Get(accountKey(norm))
	if err == badger.ErrKeyNotFound {
		// Legacy keys may have been stored with mixed-case EIP-55 address.
		// Fall back to exact key, then callers using setAccountTxn will rewrite to lowercase.
		if norm != addr {
			item, err = txn.Get([]byte("acc:" + addr))
		}
		if err == badger.ErrKeyNotFound {
			return acc, nil
		}
	}
	if err != nil {
		return acc, err
	}
	err = item.Value(func(val []byte) error { return json.Unmarshal(val, &acc) })
	return acc, err
}

func setAccountTxn(txn *badger.Txn, addr string, acc Account) error {
	b, err := json.Marshal(acc)
	if err != nil {
		return err
	}
	return txn.Set(accountKey(addr), b)
}

func (s *Store) GetBucket(name string) (genesis.Bucket, error) {
	var b genesis.Bucket
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(bucketKey(name))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { return json.Unmarshal(val, &b) })
	})
	return b, err
}

// mintFromBucketTxn increments bucket.minted inside an existing Badger txn.
// amount is in the same unit as genesis bucket caps (human EAST).
func mintFromBucketTxn(txn *badger.Txn, name string, amount int64) error {
	if amount <= 0 {
		return fmt.Errorf("mint amount must be > 0")
	}
	item, err := txn.Get(bucketKey(name))
	if err != nil {
		return fmt.Errorf("bucket %s not found", name)
	}
	var b genesis.Bucket
	if err := item.Value(func(val []byte) error { return json.Unmarshal(val, &b) }); err != nil {
		return err
	}
	if b.Minted+amount > b.Cap {
		return fmt.Errorf("bucket %s would exceed cap (%d/%d)", name, b.Minted+amount, b.Cap)
	}
	b.Minted += amount
	val, err := json.Marshal(b)
	if err != nil {
		return err
	}
	return txn.Set(bucketKey(name), val)
}

// MintFromBucket is a standalone helper (admin/tests). Prefer ApplyTx claim_* paths
// so mint + balance credit stay atomic.
func (s *Store) MintFromBucket(name string, amount int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return mintFromBucketTxn(txn, name, amount)
	})
}

// BucketRemaining returns cap - minted for a supply bucket (human EAST).
func (s *Store) BucketRemaining(name string) (int64, error) {
	b, err := s.GetBucket(name)
	if err != nil {
		return 0, err
	}
	rem := b.Cap - b.Minted
	if rem < 0 {
		return 0, nil
	}
	return rem, nil
}

func (s *Store) ListBuckets() ([]genesis.Bucket, error) {
	var out []genesis.Bucket
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("bucket:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var b genesis.Bucket
			if err := it.Item().Value(func(val []byte) error { return json.Unmarshal(val, &b) }); err != nil {
				return err
			}
			out = append(out, b)
		}
		return nil
	})
	return out, err
}

func (s *Store) ApplyTx(t *tx.Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(txn *badger.Txn) error {
		fromAcc, err := getAccountTxn(txn, t.From)
		if err != nil {
			return err
		}
		if t.Nonce > 0 && t.Nonce <= fromAcc.Nonce {
			return fmt.Errorf("invalid nonce: got %d, current %d", t.Nonce, fromAcc.Nonce)
		}

		switch t.Type {
		case tx.TxTransfer:
			if fromAcc.Balance < t.Amount {
				return fmt.Errorf("insufficient balance")
			}
			fromAcc.Balance -= t.Amount
			fromAcc.Nonce = t.Nonce
			toAcc, err := getAccountTxn(txn, t.To)
			if err != nil {
				return err
			}
			toAcc.Balance += t.Amount
			if err := setAccountTxn(txn, t.From, fromAcc); err != nil {
				return err
			}
			return setAccountTxn(txn, t.To, toAcc)

		case tx.TxStake:
			if fromAcc.Balance < t.Amount {
				return fmt.Errorf("insufficient balance to stake")
			}
			fromAcc.Balance -= t.Amount
			fromAcc.Staked += t.Amount
			fromAcc.Nonce = t.Nonce
			return setAccountTxn(txn, t.From, fromAcc)

		case tx.TxRequestUnstake:
			if fromAcc.Staked < t.Amount {
				return fmt.Errorf("insufficient staked amount")
			}
			fromAcc.Staked -= t.Amount
			fromAcc.PendingUnstake += t.Amount
			fromAcc.Nonce = t.Nonce
			return setAccountTxn(txn, t.From, fromAcc)

		case tx.TxClaimUnstake:
			if fromAcc.PendingUnstake < t.Amount {
				return fmt.Errorf("insufficient pending unstake")
			}
			fromAcc.PendingUnstake -= t.Amount
			fromAcc.Balance += t.Amount
			fromAcc.Nonce = t.Nonce
			return setAccountTxn(txn, t.From, fromAcc)

		case tx.TxClaimMining:
			// Amount is human EAST (matches genesis bucket caps). Credit balance in 6-dec subunits.
			p, err := t.ParseClaimMiningPayload()
			if err != nil {
				return err
			}
			if err := mintFromBucketTxn(txn, p.Bucket, t.Amount); err != nil {
				return err
			}
			credit := t.Amount * tx.SubunitsPerEAST
			if t.Amount > 0 && credit/t.Amount != tx.SubunitsPerEAST {
				return fmt.Errorf("claim_mining amount overflow converting to subunits")
			}
			fromAcc.Balance += credit
			fromAcc.Nonce = t.Nonce
			return setAccountTxn(txn, t.From, fromAcc)

		default:
			return fmt.Errorf("unsupported tx type: %s", t.Type)
		}
	})
}

func (s *Store) SaveBlock(h BlockHeader) error {
	return s.db.Update(func(txn *badger.Txn) error {
		b, err := json.Marshal(h)
		if err != nil {
			return err
		}
		if err := txn.Set(blockKey(h.Height), b); err != nil {
			return err
		}
		return txn.Set(metaKey("latest_height"), []byte(fmt.Sprintf("%d", h.Height)))
	})
}

func (s *Store) GetLatestHeight() (uint64, error) {
	var height uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey("latest_height"))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			_, err := fmt.Sscanf(string(val), "%d", &height)
			return err
		})
	})
	return height, err
}

func (s *Store) GetBlock(height uint64) (*BlockHeader, error) {
	var h BlockHeader
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(blockKey(height))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { return json.Unmarshal(val, &h) })
	})
	if err != nil {
		return nil, err
	}
	return &h, nil
}

func blockTxsKey(height uint64) []byte {
	return []byte(fmt.Sprintf("blktxs:%020d", height))
}

func txIndexKey(hash string) []byte {
	h := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(hash), "0x"))
	return []byte("tx:" + h)
}

// StoredTx is full tx body plus inclusion height for GET /tx and block detail.
type StoredTx struct {
	*tx.Transaction
	Height uint64 `json:"height"`
	Hash   string `json:"tx_hash"`
}

// SaveBlockWithTxs writes header + full transaction bodies (for explorer / Received index).
func (s *Store) SaveBlockWithTxs(h BlockHeader, txs []*tx.Transaction) error {
	return s.db.Update(func(txn *badger.Txn) error {
		b, err := json.Marshal(h)
		if err != nil {
			return err
		}
		if err := txn.Set(blockKey(h.Height), b); err != nil {
			return err
		}
		if err := txn.Set(metaKey("latest_height"), []byte(fmt.Sprintf("%d", h.Height))); err != nil {
			return err
		}
		// Full bodies
		bodies := make([]StoredTx, 0, len(txs))
		for _, t := range txs {
			if t == nil {
				continue
			}
			hash := "0x" + t.Hash()
			st := StoredTx{Transaction: t, Height: h.Height, Hash: hash}
			bodies = append(bodies, st)
			raw, err := json.Marshal(st)
			if err != nil {
				return err
			}
			if err := txn.Set(txIndexKey(hash), raw); err != nil {
				return err
			}
		}
		tb, err := json.Marshal(bodies)
		if err != nil {
			return err
		}
		return txn.Set(blockTxsKey(h.Height), tb)
	})
}

// GetBlockTransactions returns full txs included at height (may be empty).
func (s *Store) GetBlockTransactions(height uint64) ([]StoredTx, error) {
	var out []StoredTx
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(blockTxsKey(height))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { return json.Unmarshal(val, &out) })
	})
	return out, err
}

// GetTransaction looks up a sealed tx by hash (with or without 0x).
func (s *Store) GetTransaction(hash string) (*StoredTx, error) {
	var st StoredTx
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(txIndexKey(hash))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { return json.Unmarshal(val, &st) })
	})
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *Store) PruneOldBlocks(keep int) error {
	if keep <= 0 {
		return nil
	}
	latest, err := s.GetLatestHeight()
	if err != nil || latest <= uint64(keep) {
		return err
	}
	cutoff := latest - uint64(keep)
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte("blk:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			var height uint64
			fmt.Sscanf(string(key), "blk:%d", &height)
			if height < cutoff {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// SeedBalance sets only free balance (legacy). Prefer SeedAccount for migrations.
func (s *Store) SeedBalance(addr string, balance int64) error {
	return s.SeedAccount(addr, balance, 0, 0, "balance_only")
}

// SeedAccount writes balance / staked / pending_unstake in 6-decimal subunits.
// mode: "overwrite" | "merge_max" | "balance_only"
// Nonce is never modified (safe for in-flight txs).
func (s *Store) SeedAccount(addr string, balance, staked, pendingUnstake int64, mode string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		acc, err := getAccountTxn(txn, addr)
		if err != nil {
			return err
		}
		m := strings.ToLower(strings.TrimSpace(mode))
		switch m {
		case "balance_only", "balance":
			acc.Balance = balance
		case "merge_max", "merge":
			if balance > acc.Balance {
				acc.Balance = balance
			}
			if staked > acc.Staked {
				acc.Staked = staked
			}
			if pendingUnstake > acc.PendingUnstake {
				acc.PendingUnstake = pendingUnstake
			}
		default: // overwrite / set / empty
			acc.Balance = balance
			acc.Staked = staked
			acc.PendingUnstake = pendingUnstake
		}
		return setAccountTxn(txn, addr, acc)
	})
}

func (s *Store) Stats() map[string]any {
	latest, _ := s.GetLatestHeight()
	lsm, vlog := s.db.Size()
	return map[string]any{
		"latest_height":         latest,
		"chain_id":              "eastchain-1",
		"numeric_chain_id":      s.numericChainID,
		"total_max_supply":      s.totalMaxSupply,
		"min_validator_stake":   s.minValidatorStake,
		"chain_signing_address": s.chainSigningAddr,
		"lsm_size":              lsm,
		"vlog_size":             vlog,
		"time":                  time.Now().UTC().Format(time.RFC3339),
	}
}
