package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/state"
)

// PullAndImportSnapshot downloads GET {base}/admin/snapshot from the primary
// node and imports accounts/buckets/headers into the local store.
// Requires STATE_SYNC_URL and the same API_SECRET as the primary (X-API-Secret).
func PullAndImportSnapshot(ctx context.Context, store *state.Store) error {
	base := strings.TrimSpace(os.Getenv("STATE_SYNC_URL"))
	if base == "" {
		return nil // disabled
	}
	base = strings.TrimRight(base, "/")
	secret := strings.TrimSpace(os.Getenv("API_SECRET"))
	if secret == "" {
		secret = strings.TrimSpace(os.Getenv("EAST_VALIDATOR_API_SECRET"))
	}
	if secret == "" {
		return fmt.Errorf("STATE_SYNC_URL set but API_SECRET missing")
	}

	force := os.Getenv("STATE_SYNC_FORCE") == "true" || os.Getenv("STATE_SYNC_FORCE") == "1"
	maxBlocks := "2000"
	if v := os.Getenv("STATE_SYNC_MAX_BLOCKS"); v != "" {
		maxBlocks = v
	}
	url := fmt.Sprintf("%s/admin/snapshot?max_blocks=%s", base, maxBlocks)

	log.Info().Str("url", url).Bool("force", force).Msg("state-sync: pulling snapshot from primary")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Secret", secret)

	client := &http.Client{Timeout: 120 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("state-sync GET: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("state-sync GET status %d", res.StatusCode)
	}

	var snap state.BackupSnapshot
	if err := json.NewDecoder(res.Body).Decode(&snap); err != nil {
		return fmt.Errorf("state-sync decode: %w", err)
	}
	n, err := store.ImportSnapshot(&snap, force)
	if err != nil {
		return err
	}
	log.Info().
		Int("accounts_applied", n).
		Int("accounts_in_snap", len(snap.Accounts)).
		Uint64("snap_height", snap.LatestHeight).
		Msg("state-sync: import complete")
	return nil
}
