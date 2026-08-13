package mempool

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/eastchain/east-validator/internal/state"
	"github.com/eastchain/east-validator/internal/tx"
)

// Config mirrors CometBFT flood-mempool knobs (simplified).
type Config struct {
	MaxTxs      int   // max number of txs in the pool
	MaxTxBytes  int   // max size of a single tx (approx JSON length)
	CacheSize   int   // seen-hash LRU capacity
}

func DefaultConfig() Config {
	return Config{
		MaxTxs:     5000,
		MaxTxBytes: 64 * 1024,
		CacheSize:  10000,
	}
}

// Mempool holds uncommitted transactions (CometBFT-style flood pool, single-node friendly).
type Mempool struct {
	mu     sync.Mutex
	cfg    Config
	store  *state.Store
	order  []string                       // insertion order of hashes
	byHash map[string]*tx.Transaction
	seen   map[string]time.Time           // recently seen hashes (dedup)
}

func New(store *state.Store, cfg Config) *Mempool {
	if cfg.MaxTxs <= 0 {
		cfg = DefaultConfig()
	}
	return &Mempool{
		cfg:    cfg,
		store:  store,
		byHash: make(map[string]*tx.Transaction),
		seen:   make(map[string]time.Time),
	}
}

// CheckTx validates a tx against current state without inserting it.
// Equivalent to CometBFT ABCI CheckTx (simplified).
func (m *Mempool) CheckTx(t *tx.Transaction) error {
	if t == nil {
		return fmt.Errorf("nil transaction")
	}
	if err := t.ValidateBasic(); err != nil {
		return err
	}
	// Approx size guard
	if m.cfg.MaxTxBytes > 0 && len(t.Hash()) > 0 {
		// cheap size proxy: from+to+payload presence
		if len(t.From)+len(t.To)+len(t.Signature) > m.cfg.MaxTxBytes {
			return fmt.Errorf("tx too large")
		}
	}
	acc, err := m.store.GetAccount(t.From)
	if err != nil {
		return err
	}
	// Same fix as state.go's ApplyTx — nonce == 0 must NOT bypass this
	// check. See the comment there for why (replay attack via nonce=0).
	if t.Nonce <= acc.Nonce {
		return fmt.Errorf("invalid nonce: got %d, current %d", t.Nonce, acc.Nonce)
	}
	switch t.Type {
	case tx.TxTransfer:
		if acc.Balance < t.Amount {
			return fmt.Errorf("insufficient balance")
		}
	case tx.TxStake:
		if acc.Balance < t.Amount {
			return fmt.Errorf("insufficient balance to stake")
		}
	case tx.TxRequestUnstake:
		if acc.Staked < t.Amount {
			return fmt.Errorf("insufficient staked amount")
		}
	case tx.TxClaimUnstake:
		if acc.PendingUnstake < t.Amount {
			return fmt.Errorf("insufficient pending unstake")
		}
	case tx.TxClaimMining:
		// ValidateBasic already checked oracle sig + payload.
		// Ensure mining bucket still has room (human EAST units).
		rem, err := m.store.BucketRemaining("mining")
		if err != nil {
			return fmt.Errorf("mining bucket: %w", err)
		}
		if t.Amount > rem {
			return fmt.Errorf("mining bucket insufficient remaining: need %d, have %d", t.Amount, rem)
		}
	}
	return nil
}

// Add runs CheckTx then inserts into the pool (idempotent on hash).
func (m *Mempool) Add(t *tx.Transaction) (string, error) {
	if err := m.CheckTx(t); err != nil {
		return "", err
	}
	h := t.Hash()

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.byHash[h]; ok {
		return h, nil // already in pool
	}
	if _, ok := m.seen[h]; ok {
		return h, fmt.Errorf("tx already seen")
	}
	if len(m.byHash) >= m.cfg.MaxTxs {
		return "", fmt.Errorf("mempool full")
	}

	m.byHash[h] = t
	m.order = append(m.order, h)
	m.seen[h] = time.Now()
	m.pruneSeenLocked()
	return h, nil
}

// Peek returns up to limit txs in insertion order without removing them.
func (m *Mempool) Peek(limit int) []*tx.Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 || limit > len(m.order) {
		limit = len(m.order)
	}
	out := make([]*tx.Transaction, 0, limit)
	for _, h := range m.order {
		if len(out) >= limit {
			break
		}
		if t, ok := m.byHash[h]; ok {
			out = append(out, t)
		}
	}
	return out
}

// Take removes and returns up to limit txs for inclusion in a block.
func (m *Mempool) Take(limit int) []*tx.Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit <= 0 {
		limit = len(m.order)
	}
	out := make([]*tx.Transaction, 0, limit)
	remain := make([]string, 0, len(m.order))
	for _, h := range m.order {
		if len(out) < limit {
			if t, ok := m.byHash[h]; ok {
				out = append(out, t)
				delete(m.byHash, h)
				continue
			}
		}
		if _, ok := m.byHash[h]; ok {
			remain = append(remain, h)
		}
	}
	m.order = remain
	return out
}

// RemoveByHash drops txs that were committed (or rejected).
func (m *Mempool) RemoveByHash(hashes []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		set[h] = struct{}{}
		delete(m.byHash, h)
	}
	remain := m.order[:0]
	for _, h := range m.order {
		if _, drop := set[h]; !drop {
			if _, ok := m.byHash[h]; ok {
				remain = append(remain, h)
			}
		}
	}
	m.order = remain
}

func (m *Mempool) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.byHash)
}

func (m *Mempool) Stats() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return map[string]any{
		"size":     len(m.byHash),
		"max_txs":  m.cfg.MaxTxs,
		"seen":     len(m.seen),
		"cache":    m.cfg.CacheSize,
	}
}

func (m *Mempool) pruneSeenLocked() {
	if len(m.seen) <= m.cfg.CacheSize {
		return
	}
	target := m.cfg.CacheSize * 3 / 4

	type entry struct {
		hash string
		ts   time.Time
	}
	entries := make([]entry, 0, len(m.seen))
	for h, ts := range m.seen {
		entries = append(entries, entry{h, ts})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ts.Before(entries[j].ts) })

	toDrop := len(entries) - target
	for i := 0; i < toDrop; i++ {
		delete(m.seen, entries[i].hash)
	}
}
