package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
)

// BackupSnapshot is a portable JSON dump of critical chain metadata + accounts
// + recent headers. Not a full Badger copy — use for disaster recovery seeding
// and cross-env migration. For bit-perfect restore, stop the node and copy DATA_DIR.
type BackupSnapshot struct {
	Version        int                    `json:"version"`
	CreatedAt      string                 `json:"created_at"`
	LatestHeight   uint64                 `json:"latest_height"`
	NumericChainID int64                  `json:"numeric_chain_id"`
	TotalMaxSupply int64                  `json:"total_max_supply"`
	ValidatorSet   *ValidatorSetRecord    `json:"validator_set,omitempty"`
	Buckets        []json.RawMessage      `json:"buckets,omitempty"`
	Accounts       map[string]Account     `json:"accounts"`
	RecentBlocks   []BlockHeader          `json:"recent_blocks"`
	Jailed         []JailRecord           `json:"jailed,omitempty"`
}

// ExportBackup writes a JSON snapshot under dataDir/backups/.
func (s *Store) ExportBackup(maxBlocks int) (string, error) {
	if maxBlocks <= 0 {
		maxBlocks = 500
	}
	latest, err := s.GetLatestHeight()
	if err != nil {
		return "", err
	}
	vset, _ := s.GetValidatorSet()
	jailed, _ := s.ListJailed(200)

	accounts := make(map[string]Account)
	var buckets []json.RawMessage
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			if len(key) > 4 && key[:4] == "acc:" {
				var acc Account
				if err := it.Item().Value(func(val []byte) error {
					return json.Unmarshal(val, &acc)
				}); err != nil {
					return err
				}
				accounts[key[4:]] = acc
			}
			if len(key) > 7 && key[:7] == "bucket:" {
				var raw []byte
				if err := it.Item().Value(func(val []byte) error {
					raw = append([]byte{}, val...)
					return nil
				}); err != nil {
					return err
				}
				buckets = append(buckets, json.RawMessage(raw))
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	var blocks []BlockHeader
	start := uint64(1)
	if latest > uint64(maxBlocks) {
		start = latest - uint64(maxBlocks) + 1
	}
	for h := start; h <= latest; h++ {
		b, err := s.GetBlock(h)
		if err != nil {
			continue
		}
		blocks = append(blocks, *b)
	}

	snap := BackupSnapshot{
		Version:        1,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
		LatestHeight:   latest,
		NumericChainID: s.numericChainID,
		TotalMaxSupply: s.totalMaxSupply,
		ValidatorSet:   vset,
		Buckets:        buckets,
		Accounts:       accounts,
		RecentBlocks:   blocks,
		Jailed:         jailed,
	}

	dir := filepath.Join(s.path, "backups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	name := fmt.Sprintf("backup-%d.json", time.Now().Unix())
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(snap); err != nil {
		return "", err
	}
	log.Info().Str("path", path).Uint64("height", latest).Int("accounts", len(accounts)).Msg("backup exported")
	return path, nil
}

// RestoreAccountsFromSnapshot seeds account balances from a backup JSON file
// without rewriting block history (safe for Neon→validator migration of balances).
// Only applies accounts that are missing or have zero balance unless force=true.
func (s *Store) RestoreAccountsFromSnapshot(path string, force bool) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var snap BackupSnapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return 0, err
	}
	applied := 0
	for addr, acc := range snap.Accounts {
		cur, err := s.GetAccount(addr)
		if err != nil {
			return applied, err
		}
		if !force && (cur.Balance != 0 || cur.Staked != 0 || cur.Nonce != 0) {
			continue
		}
		if err := s.SeedBalance(addr, acc.Balance); err != nil {
			return applied, err
		}
		// Also restore stake fields via direct write
		_ = s.db.Update(func(txn *badger.Txn) error {
			full := Account{
				Balance:        acc.Balance,
				Staked:         acc.Staked,
				PendingUnstake: acc.PendingUnstake,
				Nonce:          acc.Nonce,
			}
			val, _ := json.Marshal(full)
			return txn.Set(accountKey(addr), val)
		})
		applied++
	}
	if snap.ValidatorSet != nil && len(snap.ValidatorSet.Addresses) > 0 {
		_, _ = s.SaveValidatorSet(snap.ValidatorSet.Addresses)
	}
	log.Info().Int("accounts", applied).Str("path", path).Msg("accounts restored from snapshot")
	return applied, nil
}

// BuildSnapshot builds an in-memory BackupSnapshot (same content as ExportBackup).
func (s *Store) BuildSnapshot(maxBlocks int) (*BackupSnapshot, error) {
	if maxBlocks <= 0 {
		maxBlocks = 500
	}
	latest, err := s.GetLatestHeight()
	if err != nil {
		return nil, err
	}
	vset, _ := s.GetValidatorSet()
	jailed, _ := s.ListJailed(200)

	accounts := make(map[string]Account)
	var buckets []json.RawMessage
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			if len(key) > 4 && key[:4] == "acc:" {
				var acc Account
				if err := it.Item().Value(func(val []byte) error {
					return json.Unmarshal(val, &acc)
				}); err != nil {
					return err
				}
				accounts[key[4:]] = acc
			}
			if len(key) > 7 && key[:7] == "bucket:" {
				var raw []byte
				if err := it.Item().Value(func(val []byte) error {
					raw = append([]byte{}, val...)
					return nil
				}); err != nil {
					return err
				}
				buckets = append(buckets, json.RawMessage(raw))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var blocks []BlockHeader
	start := uint64(1)
	if latest > uint64(maxBlocks) {
		start = latest - uint64(maxBlocks) + 1
	}
	for h := start; h <= latest; h++ {
		b, err := s.GetBlock(h)
		if err != nil {
			continue
		}
		blocks = append(blocks, *b)
	}

	return &BackupSnapshot{
		Version:        1,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
		LatestHeight:   latest,
		NumericChainID: s.numericChainID,
		TotalMaxSupply: s.totalMaxSupply,
		ValidatorSet:   vset,
		Buckets:        buckets,
		Accounts:       accounts,
		RecentBlocks:   blocks,
		Jailed:         jailed,
	}, nil
}

// ImportSnapshot applies accounts (balance+stake+nonce), buckets, and optional
// recent headers from a BackupSnapshot. force=true overwrites existing accounts.
func (s *Store) ImportSnapshot(snap *BackupSnapshot, force bool) (accountsApplied int, err error) {
	if snap == nil {
		return 0, fmt.Errorf("nil snapshot")
	}
	for addr, acc := range snap.Accounts {
		cur, err := s.GetAccount(addr)
		if err != nil {
			return accountsApplied, err
		}
		if !force && (cur.Balance != 0 || cur.Staked != 0 || cur.Nonce != 0) {
			continue
		}
		if err := s.db.Update(func(txn *badger.Txn) error {
			full := Account{
				Balance:        acc.Balance,
				Staked:         acc.Staked,
				PendingUnstake: acc.PendingUnstake,
				Nonce:          acc.Nonce,
			}
			val, err := json.Marshal(full)
			if err != nil {
				return err
			}
			return txn.Set(accountKey(addr), val)
		}); err != nil {
			return accountsApplied, err
		}
		accountsApplied++
	}

	// Restore bucket mint counters (raw JSON as stored).
	for _, raw := range snap.Buckets {
		var b struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &b); err != nil || b.Name == "" {
			continue
		}
		_ = s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(bucketKey(b.Name), []byte(raw))
		})
	}

	// Fill missing headers only (do not fork tip if we already have a different hash).
	for _, hdr := range snap.RecentBlocks {
		if hdr.Height == 0 {
			continue
		}
		if existing, err := s.GetBlock(hdr.Height); err == nil && existing != nil {
			continue
		}
		_ = s.SaveBlock(hdr)
	}

	if snap.ValidatorSet != nil && len(snap.ValidatorSet.Addresses) > 0 {
		_, _ = s.SaveValidatorSet(snap.ValidatorSet.Addresses)
	}

	log.Info().
		Int("accounts", accountsApplied).
		Uint64("snap_height", snap.LatestHeight).
		Int("buckets", len(snap.Buckets)).
		Int("headers", len(snap.RecentBlocks)).
		Msg("snapshot imported")
	return accountsApplied, nil
}
