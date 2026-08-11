package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/api"
	"github.com/eastchain/east-validator/internal/config"
	"github.com/eastchain/east-validator/internal/consensus"
	"github.com/eastchain/east-validator/internal/genesis"
	"github.com/eastchain/east-validator/internal/mempool"
	"github.com/eastchain/east-validator/internal/p2p"
	"github.com/eastchain/east-validator/internal/state"
	chainsync "github.com/eastchain/east-validator/internal/sync"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	cfg := config.Load()
	bftEnabled := os.Getenv("BFT_ENABLED") != "false"

	log.Info().
		Str("node_id", cfg.NodeID).
		Str("data_dir", cfg.DataDir).
		Str("http", cfg.HTTPAddr).
		Dur("block_interval", cfg.BlockInterval).
		Bool("auto_produce", cfg.AutoProduce).
		Bool("bft_enabled", bftEnabled).
		Msg("starting east-validator (sealer mode)")

	store, err := state.Open(cfg.DataDir)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open state")
	}
	defer store.Close()

	var g *genesis.Genesis
	if _, err := os.Stat(cfg.GenesisPath); err == nil {
		g, err = genesis.LoadFile(cfg.GenesisPath)
		if err != nil {
			log.Fatal().Err(err).Msg("invalid genesis file")
		}
		log.Info().
			Str("path", cfg.GenesisPath).
			Int64("numeric_chain_id", g.NumericChainID).
			Msg("loaded genesis file")
	} else {
		def := genesis.Default()
		if addr := os.Getenv("CHAIN_SIGNING_ADDRESS"); addr != "" {
			def.ChainSigningAddress = addr
		}
		g = &def
		log.Info().
			Int64("numeric_chain_id", g.NumericChainID).
			Msg("using default genesis (1B max supply + tokenomics buckets)")
	}

	if err := store.InitGenesis(g); err != nil {
		log.Fatal().Err(err).Msg("genesis init failed")
	}

	localAddr := os.Getenv("CHAIN_SIGNING_ADDRESS")
	if localAddr == "" && os.Getenv("CHAIN_SIGNING_PRIVATE_KEY") != "" {
		// derived later in producer / BFT if needed
	}

	leader := consensus.NewLeaderSchedule(localAddr)
	leader.SetStore(store)
	if raw := os.Getenv("VALIDATORS"); raw != "" {
		parts := strings.Split(raw, ",")
		var addrs []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				addrs = append(addrs, p)
			}
		}
		leader.SetValidators(addrs)
		log.Info().Int("count", len(addrs)).Msg("validator set loaded from VALIDATORS env (persisted)")
	} else if leader.LoadFromStore() {
		log.Info().Int("count", leader.Count()).Msg("validator set loaded from local store")
	} else if localAddr != "" {
		leader.SetValidators([]string{localAddr})
		log.Info().Str("local", localAddr).Msg("validator set: solo self-registration")
	}

	pool := mempool.New(store, mempool.DefaultConfig())

	p2pCfg := p2p.LoadConfigFromEnv(cfg.NodeID)
	p2pNode, err := p2p.New(p2pCfg)
	if err != nil {
		log.Fatal().Err(err).Msg("p2p start failed")
	}
	defer p2pNode.Close()

	p2pNode.OnHeartbeat(func(m p2p.HeartbeatMsg) {
		tier := state.TierLight
		if m.Tier == "full" {
			tier = state.TierFull
		}
		if m.Address == "" {
			return
		}
		if _, err := store.RecordHeartbeat(m.Address, m.NodeID, tier, cfg.EpochSeconds); err != nil {
			log.Debug().Err(err).Msg("record remote heartbeat")
		}
	})

	p2pNode.RegisterSyncHandler(p2p.StoreBlockProvider{Store: store})

	if os.Getenv("SYNC_BEFORE_CONSENSUS") != "false" {
		runStartupCatchUp(store, p2pNode)
	}

	var bft *consensus.BFTEngine
	if bftEnabled {
		bftCfg := consensus.DefaultBFTConfig()
		bftCfg.MinBlockInterval = cfg.BlockInterval
		if bftCfg.MinBlockInterval <= 0 {
			bftCfg.MinBlockInterval = 120 * time.Second
		}
		if v := os.Getenv("BFT_PROPOSE_TIMEOUT_SEC"); v != "" {
			if n, err := time.ParseDuration(v + "s"); err == nil && n > 0 {
				bftCfg.ProposeTimeout = n
			}
		}
		if v := os.Getenv("BFT_PREVOTE_TIMEOUT_SEC"); v != "" {
			if n, err := time.ParseDuration(v + "s"); err == nil && n > 0 {
				bftCfg.PrevoteTimeout = n
			}
		}
		if v := os.Getenv("BFT_PRECOMMIT_TIMEOUT_SEC"); v != "" {
			if n, err := time.ParseDuration(v + "s"); err == nil && n > 0 {
				bftCfg.PrecommitTimeout = n
			}
		}
		bftCfg.Enabled = true
		bft = consensus.NewBFTEngine(store, leader, bftCfg)
		bft.SetMempool(pool)
		bft.SetP2P(p2pNode)
		bft.Start()
		defer bft.Stop()

		p2pNode.OnConsensus(func(m p2p.ConsensusMsg) {
			switch m.Kind {
			case p2p.ConsensusKindProposal:
				if m.Proposal != nil {
					bft.HandleProposal(consensus.ProposalFromP2P(m.Proposal))
				}
			case p2p.ConsensusKindVote:
				if m.Vote != nil {
					bft.HandleVote(consensus.VoteFromP2P(m.Vote))
				}
			case p2p.ConsensusKindCommit:
				if m.Commit != nil {
					log.Debug().
						Uint64("height", m.Commit.Height).
						Str("hash", m.Commit.BlockHash).
						Int("votes", m.Commit.VoteCount).
						Msg("BFT commit cert received")
				}
			}
		})
	}

	var producer *consensus.Producer
	if cfg.AutoProduce && !bftEnabled {
		producer = consensus.NewProducer(store, cfg.BlockInterval)
		producer.SetP2P(p2pNode)
		producer.SetMempool(pool)
		producer.SetLeader(leader)
		producer.Start()
		defer producer.Stop()
	}

	p2pNode.OnBlock(func(a p2p.BlockAnnounce) {
		localH, _ := store.GetLatestHeight()
		if a.Height > localH+1 {
			log.Info().
				Uint64("local", localH).
				Uint64("peer_height", a.Height).
				Str("from", a.FromPeer).
				Msg("gossip block ahead of local tip — scheduling catch-up (no peer penalty)")
			go func(target uint64) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				// Prefer HTTP if configured (works even when PeerCount is wrong).
				if tipURL := strings.TrimSpace(os.Getenv("CATCHUP_TIP_URL")); tipURL != "" {
					if err := syncHeadersHTTP(ctx, store, tipURL, target); err != nil {
						log.Debug().Err(err).Msg("runtime HTTP catch-up failed")
					}
					return
				}
				if err := chainsync.SyncFromPeers(ctx, store, p2pNode, target); err != nil {
					log.Debug().Err(err).Uint64("target", target).Msg("runtime P2P catch-up failed")
				}
			}(a.Height)
			return
		}

		err := consensus.VerifyAndSaveGossipedBlockStrict(store, consensus.GossipBlockInput{
			Height:           a.Height,
			Hash:             a.Hash,
			PrevHash:         a.PrevHash,
			Timestamp:        a.Timestamp,
			Proposer:         a.Proposer,
			Signature:        a.Signature,
			Txs:              a.Txs,
			AllowedProposers: leader.Validators(),
		})
		if err != nil {
			log.Debug().Err(err).Uint64("height", a.Height).Str("from", a.FromPeer).Msg("gossiped block rejected")
			msg := err.Error()
			if strings.Contains(msg, "stale or out of order") || strings.Contains(msg, "!= expected") {
				p2pNode.ReportPeer(a.FromPeer, -1, "height_mismatch")
			} else {
				p2pNode.ReportPeer(a.FromPeer, -10, "invalid_block")
			}
			return
		}
		p2pNode.ReportPeer(a.FromPeer, +2, "valid_block")
		if producer != nil {
			producer.NotifyExternalProposal()
		}
		log.Info().
			Uint64("height", a.Height).
			Str("hash", a.Hash).
			Str("from", a.FromPeer).
			Int("txs", len(a.Txs)).
			Msg("p2p block received and applied")
	})

	srv := api.New(cfg, store, producer, p2pNode, pool, leader)
	srv.SetBFT(bft)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Info().Msg("shutting down...")
		if bft != nil {
			bft.Stop()
		}
		if producer != nil {
			producer.Stop()
		}
		_ = p2pNode.Close()
		_ = srv.Shutdown()
		_ = store.Close()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("http server failed")
	}
}

// livePeerCount uses the libp2p host mesh (authoritative), not the internal
// peerCount counter which can miss bootstrap connects that race NotifyBundle.
func livePeerCount(node *p2p.Node) int {
	if node == nil || node.Host() == nil {
		return 0
	}
	return len(node.Host().Network().Peers())
}

func runStartupCatchUp(store *state.Store, node *p2p.Node) {
	localH, err := store.GetLatestHeight()
	if err != nil {
		log.Warn().Err(err).Msg("catch-up: cannot read local height")
		return
	}

	// 1) Always try HTTP tip first (does not need P2P peers).
	tipURL := strings.TrimSpace(os.Getenv("CATCHUP_TIP_URL"))
	target := fetchTipHeight(tipURL)
	if target > localH && tipURL != "" {
		log.Info().Uint64("local", localH).Uint64("target", target).Str("url", tipURL).
			Msg("catch-up: syncing headers via HTTP from CATCHUP_TIP_URL")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := syncHeadersHTTP(ctx, store, tipURL, target); err != nil {
			log.Warn().Err(err).Msg("catch-up: HTTP header sync failed")
		} else {
			after, _ := store.GetLatestHeight()
			log.Info().Uint64("height", after).Msg("catch-up: HTTP sync complete")
			localH = after
			if localH >= target {
				return
			}
		}
	}

	// 2) Wait briefly for live libp2p peers, then P2P blocksync.
	waitSec := 45
	if v := os.Getenv("SYNC_PEER_WAIT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			waitSec = n
		}
	}
	log.Info().Int("wait_sec", waitSec).Msg("catch-up: waiting for live P2P peers")
	deadline := time.Now().Add(time.Duration(waitSec) * time.Second)
	for time.Now().Before(deadline) {
		if livePeerCount(node) > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	nPeers := livePeerCount(node)
	if nPeers == 0 {
		log.Warn().Uint64("local", localH).Msg("catch-up: no live P2P peers — consensus starts from local tip")
		return
	}
	log.Info().Int("peers", nPeers).Msg("catch-up: live peers detected")

	if target == 0 {
		target = probePeerTip(node, localH)
	}
	if target <= localH {
		log.Info().Uint64("local", localH).Uint64("target", target).Msg("catch-up: already at tip")
		return
	}

	log.Info().Uint64("local", localH).Uint64("target", target).Msg("catch-up: P2P blocksync")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := chainsync.SyncFromPeers(ctx, store, node, target); err != nil {
		log.Warn().Err(err).Msg("catch-up: SyncFromPeers failed")
		return
	}
	after, _ := store.GetLatestHeight()
	log.Info().Uint64("height", after).Msg("catch-up: P2P sync complete — starting consensus")
}

func fetchTipHeight(base string) uint64 {
	base = strings.TrimSpace(base)
	if base == "" {
		return 0
	}
	base = strings.TrimRight(base, "/")
	url := base
	if !strings.Contains(base, "/block/") {
		url = base + "/block/latest"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Warn().Err(err).Str("url", url).Msg("catch-up: tip HTTP failed")
		return 0
	}
	defer res.Body.Close()
	var body struct {
		Height uint64 `json:"height"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		log.Warn().Err(err).Msg("catch-up: tip JSON decode failed")
		return 0
	}
	log.Info().Uint64("tip", body.Height).Str("url", url).Msg("catch-up: network tip from HTTP")
	return body.Height
}

// syncHeadersHTTP pulls /block/{h} from tipBase for each missing height and
// saves BlockHeader rows so local tip can advance without P2P.
func syncHeadersHTTP(ctx context.Context, store *state.Store, tipBase string, target uint64) error {
	tipBase = strings.TrimRight(strings.TrimSpace(tipBase), "/")
	if tipBase == "" {
		return fmt.Errorf("empty CATCHUP_TIP_URL")
	}
	localH, err := store.GetLatestHeight()
	if err != nil {
		return err
	}
	if localH >= target {
		return nil
	}
	client := &http.Client{Timeout: 15 * time.Second}
	saved := 0
	for h := localH + 1; h <= target; h++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		url := fmt.Sprintf("%s/block/%d", tipBase, h)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		res, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("GET %s: %w", url, err)
		}
		var hdr state.BlockHeader
		decErr := json.NewDecoder(res.Body).Decode(&hdr)
		res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("GET %s: status %d", url, res.StatusCode)
		}
		if decErr != nil {
			return fmt.Errorf("decode block %d: %w", h, decErr)
		}
		if hdr.Height == 0 {
			hdr.Height = h
		}
		current, _ := store.GetLatestHeight()
		if hdr.Height <= current {
			continue
		}
		if err := store.SaveBlock(hdr); err != nil {
			return fmt.Errorf("save block %d: %w", hdr.Height, err)
		}
		saved++
		if saved%50 == 0 {
			log.Info().Int("saved", saved).Uint64("height", hdr.Height).Msg("catch-up: HTTP progress")
		}
	}
	log.Info().Int("saved", saved).Msg("catch-up: HTTP headers saved")
	return nil
}

func probePeerTip(node *p2p.Node, localH uint64) uint64 {
	if node.Host() == nil {
		return localH
	}
	peers := node.Host().Network().Peers()
	if len(peers) == 0 {
		return localH
	}
	guess := localH + 500
	if guess < 500 {
		guess = 500
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	blocks, err := node.RequestBlocks(ctx, peers[0], localH+1, guess)
	if err != nil || len(blocks) == 0 {
		log.Debug().Err(err).Msg("catch-up: peer tip probe empty")
		return localH
	}
	var max uint64
	for _, b := range blocks {
		if b.Height > max {
			max = b.Height
		}
	}
	if len(blocks) >= 50 {
		max = max + 500
	}
	log.Info().Uint64("probed_tip", max).Msg("catch-up: tip estimated from peer blocksync")
	return max
}
