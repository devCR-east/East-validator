package main

import (
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/api"
	"github.com/eastchain/east-validator/internal/config"
	"github.com/eastchain/east-validator/internal/consensus"
	"github.com/eastchain/east-validator/internal/genesis"
	"github.com/eastchain/east-validator/internal/mempool"
	"github.com/eastchain/east-validator/internal/p2p"
	"github.com/eastchain/east-validator/internal/state"
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

	// Local proposer identity
	localAddr := os.Getenv("CHAIN_SIGNING_ADDRESS")
	if localAddr == "" && os.Getenv("CHAIN_SIGNING_PRIVATE_KEY") != "" {
		// derived later in producer / BFT if needed
	}

	// Leader schedule (CometBFT-style round-robin when 2+ validators)
	leader := consensus.NewLeaderSchedule(localAddr)
	leader.SetStore(store)
	// Prefer persisted set (survives restart); env overrides on first boot / ops change.
	if raw := os.Getenv("VALIDATORS"); raw != "" {
		parts := strings.Split(raw, ",")
		var addrs []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				addrs = append(addrs, p)
			}
		}
		leader.SetValidators(addrs) // also persists
		log.Info().Int("count", len(addrs)).Msg("validator set loaded from VALIDATORS env (persisted)")
	} else if leader.LoadFromStore() {
		log.Info().Int("count", leader.Count()).Msg("validator set loaded from local store")
	} else if localAddr != "" {
		// Solo: register self so stats are consistent
		leader.SetValidators([]string{localAddr})
		log.Info().Str("local", localAddr).Msg("validator set: solo self-registration")
	}

	// Mempool
	pool := mempool.New(store, mempool.DefaultConfig())

	// libp2p
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

	// Block-sync stream protocol so peers that fall behind can catch up
	// without relying solely on gossip (which only covers the tip).
	p2pNode.RegisterSyncHandler(p2p.StoreBlockProvider{Store: store})

	// ── BFT engine (default on) ─────────────────────────────────────
	var bft *consensus.BFTEngine
	if bftEnabled {
		bftCfg := consensus.DefaultBFTConfig()
		bftCfg.MinBlockInterval = cfg.BlockInterval
		if bftCfg.MinBlockInterval <= 0 {
			bftCfg.MinBlockInterval = 120 * time.Second
		}
		// Optional overrides (seconds). Defaults: propose/prevote/precommit = 5.
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

		// Wire P2P consensus messages into the engine
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
				// Informational — block body still arrives via TopicBlocks
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

	// Legacy auto-producer: only when BFT is disabled (backward compatible)
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
		// P0: verify sealer sig, proposer in set, full tx ValidateBasic, atomic apply
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
			p2pNode.ReportPeer(a.FromPeer, -10, "invalid_block")
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
