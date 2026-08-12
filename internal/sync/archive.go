// Package sync implements startup catch-up of block HEADERS from the
// legacy Neon/Vercel archive (via HTTP), for the height range this
// validator hasn't produced/received itself yet.
//
// Scope, deliberately narrow: this fills in state.BlockHeader rows only
// (height, hash, prev_hash, timestamp, proposer, tx_hashes) so the chain
// is structurally continuous for BFT (prevHash matching) and for anything
// reading block history (e.g. an explorer). It does NOT touch account
// balances/stake — Neon's archive only ever stored tx hashes, not full
// tx bodies, so there's nothing here to replay into state.ApplyTx. Balance
// migration is a separate, one-time concern — see ops-migrate-neon/.
package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/crypto"
	"github.com/eastchain/east-validator/internal/p2p"
	"github.com/eastchain/east-validator/internal/state"
)

const (
	archiveRangeSize = 500 // matches MAX_RANGE in the Vercel route — larger requests get clamped there anyway
	archiveTimeout   = 20 * time.Second
)

// archiveBlock mirrors BlockOut in
// src/app/api/archive/blocks-range/route.ts. Field names are the JSON
// keys Vercel actually sends — do not rename without checking that file.
type archiveBlock struct {
	Success      bool   `json:"success"`
	Height       uint64 `json:"height"`
	Hash         string `json:"hash"`
	PreviousHash string `json:"previousHash"`
	MerkleRoot   string `json:"merkleRoot"`
	Validator    string `json:"validator"`
	Timestamp    int64  `json:"timestamp"`
	Signature    string `json:"signature"`
	Transactions []struct {
		TxHash string `json:"tx_hash"`
	} `json:"transactions"`
}

type archiveRangeResponse struct {
	Blocks []archiveBlock `json:"blocks"`
	Error  string         `json:"error"`
}

type heightResponse struct {
	Height int64  `json:"height"`
	Error  string `json:"error"`
}

// FetchArchiveHeight asks Vercel for the archive's own tip height.
//
// NOTE: this deliberately does NOT call /api/chain-height — that route
// proxies back to the validator's own /block/latest (see its own comment:
// it exists so Light Nodes don't sync a stale Neon height, not to expose
// Neon's height). Calling it from the validator at startup would be
// circular: the validator has no height yet, so it would just ask itself.
// /api/archive/headers?tip=1 queries ledger.chain_meta / ledger.blocks
// directly and is the actual archive tip.
func FetchArchiveHeight(ctx context.Context, baseURL string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/archive/headers?tip=1", nil)
	if err != nil {
		return 0, err
	}
	client := &http.Client{Timeout: archiveTimeout}
	res, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("archive tip request: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("archive tip: HTTP %d: %s", res.StatusCode, string(body))
	}
	var hr heightResponse
	if err := json.Unmarshal(body, &hr); err != nil {
		return 0, fmt.Errorf("archive tip: decode: %w", err)
	}
	if hr.Error != "" {
		return 0, fmt.Errorf("archive tip: %s", hr.Error)
	}
	if hr.Height < 0 {
		// /api/archive/headers?tip=1 returns {height:-1} when the archive is empty
		return 0, fmt.Errorf("archive tip: archive is empty")
	}
	return hr.Height, nil
}

// fetchArchiveRange fetches one chunk [from, to] from Vercel's
// archive/blocks-range endpoint.
func fetchArchiveRange(ctx context.Context, baseURL string, from, to uint64) ([]archiveBlock, error) {
	url := fmt.Sprintf("%s/api/archive/blocks-range?from=%d&to=%d", baseURL, from, to)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: archiveTimeout}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("blocks-range request: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("blocks-range: HTTP %d: %s", res.StatusCode, string(body))
	}
	var rr archiveRangeResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return nil, fmt.Errorf("blocks-range: decode: %w", err)
	}
	if rr.Error != "" {
		return nil, fmt.Errorf("blocks-range: %s", rr.Error)
	}
	return rr.Blocks, nil
}

// SyncFromArchive fills in block headers for [1, targetHeight] that this
// node doesn't already have, fetched in chunks from Vercel's archive.
//
// Each block's signature is verified against expectedSigner (Vercel's
// CHAIN_SIGNING_ADDRESS) before being saved — this is archived historical
// data from a system this validator doesn't otherwise trust, so an
// unsigned or wrongly-signed entry is skipped and logged rather than
// silently accepted. If expectedSigner is empty, signature checking is
// skipped entirely (only appropriate for local/dev setups).
//
// Blocks already present locally (from a previous sync, or produced by
// this node itself) are left untouched — this checks the local height
// again immediately before each write and skips anything at or below it,
// since state.Store.SaveBlock has no such protection built in (it always
// overwrites and always advances latest_height to whatever it's given).
func SyncFromArchive(ctx context.Context, store *state.Store, baseURL, expectedSigner string, targetHeight uint64) error {
	if baseURL == "" {
		return fmt.Errorf("archive base URL is empty")
	}
	localHeight, err := store.GetLatestHeight()
	if err != nil {
		return fmt.Errorf("read local height: %w", err)
	}
	if localHeight >= targetHeight {
		log.Info().Uint64("local", localHeight).Uint64("target", targetHeight).Msg("archive sync: already caught up, nothing to do")
		return nil
	}

	log.Info().Uint64("from", localHeight+1).Uint64("to", targetHeight).Msg("archive sync: starting")

	saved := 0
	skippedBadSig := 0
	for from := localHeight + 1; from <= targetHeight; from += archiveRangeSize {
		to := from + archiveRangeSize - 1
		if to > targetHeight {
			to = targetHeight
		}

		blocks, err := fetchArchiveRange(ctx, baseURL, from, to)
		if err != nil {
			return fmt.Errorf("fetch range [%d,%d]: %w", from, to, err)
		}

		for _, b := range blocks {
			if !b.Success || b.Hash == "" {
				continue // Vercel had no data for this height (gap in Neon itself) — skip, don't fail the whole sync
			}
			if expectedSigner != "" {
				if b.Signature == "" {
					skippedBadSig++
					log.Warn().Uint64("height", b.Height).Msg("archive sync: unsigned block, skipping")
					continue
				}
				msg := crypto.BuildChainSigningMessage(b.Height, b.Hash)
				ok, err := crypto.VerifyEIP191(msg, b.Signature, expectedSigner)
				if err != nil || !ok {
					skippedBadSig++
					log.Warn().Uint64("height", b.Height).Err(err).Msg("archive sync: signature verification failed, skipping")
					continue
				}
			}

			txHashes := make([]string, 0, len(b.Transactions))
			for _, t := range b.Transactions {
				if t.TxHash != "" {
					txHashes = append(txHashes, t.TxHash)
				}
			}

			// SaveBlock in state.go unconditionally overwrites and
			// unconditionally sets latest_height to whatever height it's
			// given — it has no built-in "don't go backwards" protection.
			// Re-check right before writing so a stale archive entry can
			// never regress latest_height below what this node already
			// has (e.g. if the producer advanced height concurrently
			// while this sync was mid-flight).
			current, err := store.GetLatestHeight()
			if err != nil {
				return fmt.Errorf("recheck local height before saving block %d: %w", b.Height, err)
			}
			if b.Height <= current {
				continue
			}

			header := state.BlockHeader{
				Height:    b.Height,
				Hash:      b.Hash,
				PrevHash:  b.PreviousHash,
				TxHashes:  txHashes,
				Timestamp: b.Timestamp,
				Proposer:  b.Validator,
				TxCount:   len(txHashes),
				Signature: b.Signature,
			}
			if err := store.SaveBlock(header); err != nil {
				return fmt.Errorf("save block %d: %w", b.Height, err)
			}
			saved++
		}

		log.Info().Uint64("chunk_from", from).Uint64("chunk_to", to).Int("saved_so_far", saved).Msg("archive sync: chunk complete")
	}

	log.Info().Int("saved", saved).Int("skipped_bad_signature", skippedBadSig).Msg("archive sync: complete")
	return nil
}

// SyncFromPeers fills in block headers for (localHeight, targetHeight]
// by requesting them from any currently-connected P2P peer, in chunks
// (the block-sync protocol caps each response at 50 blocks — see
// p2p/sync.go's maxBlocksPerResponse). Like archive sync, this is
// header-only: peers reconstructing ranges from their own BlockHeader
// storage don't have historical tx bodies to send either (see
// block_provider.go's GetBlockRange doc comment), so balances are NOT
// replayed here.
//
// Requires at least one connected peer; returns an error if none are
// available (callers should treat that as "nothing more to do right now"
// rather than fatal — see the startup wiring in cmd/validator/main.go).
func SyncFromPeers(ctx context.Context, store *state.Store, node *p2p.Node, targetHeight uint64) error {
	localHeight, err := store.GetLatestHeight()
	if err != nil {
		return fmt.Errorf("read local height: %w", err)
	}
	if localHeight >= targetHeight {
		return nil
	}
	if node == nil || !node.Enabled() {
		return fmt.Errorf("p2p not enabled")
	}
	peers := node.Host().Network().Peers()
	if len(peers) == 0 {
		return fmt.Errorf("no connected peers to sync from")
	}
	peerID := peers[0] // any connected peer will do — they all serve from their own store

	log.Info().Uint64("from", localHeight+1).Uint64("to", targetHeight).Str("peer", peerID.String()).Msg("p2p catch-up sync: starting")

	saved := 0
	for from := localHeight + 1; from <= targetHeight; {
		blocks, err := node.RequestBlocks(ctx, peerID, from, targetHeight)
		if err != nil {
			return fmt.Errorf("request blocks [%d,%d] from %s: %w", from, targetHeight, peerID.String(), err)
		}
		if len(blocks) == 0 {
			break // peer has nothing more for this range
		}
		for _, b := range blocks {
			current, err := store.GetLatestHeight()
			if err != nil {
				return fmt.Errorf("recheck local height before saving block %d: %w", b.Height, err)
			}
			if b.Height <= current {
				continue
			}
			header := state.BlockHeader{
				Height:    b.Height,
				Hash:      b.Hash,
				PrevHash:  b.PrevHash,
				Timestamp: b.Timestamp,
				Proposer:  b.Proposer,
				Signature: b.Signature,
				TxHashes:  b.TxHashes,
				TxCount:   len(b.TxHashes),
			}
			if err := store.SaveBlock(header); err != nil {
				return fmt.Errorf("save block %d: %w", b.Height, err)
			}
			saved++
			from = b.Height + 1
		}
	}

	log.Info().Int("saved", saved).Msg("p2p catch-up sync: complete")
	return nil
}
