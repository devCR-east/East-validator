package consensus

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/crypto"
	"github.com/eastchain/east-validator/internal/mempool"
	"github.com/eastchain/east-validator/internal/p2p"
	"github.com/eastchain/east-validator/internal/state"
	"github.com/eastchain/east-validator/internal/tx"
)

// Producer auto-seals blocks on a fixed interval when this node is the
// scheduled leader and no external proposal just landed.
type Producer struct {
	store    *state.Store
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
	p2pNode  *p2p.Node
	pool     *mempool.Mempool
	leader   *LeaderSchedule
	maxTxs   int

	mu                   sync.Mutex
	lastExternalProposal time.Time
}

func NewProducer(store *state.Store, interval time.Duration) *Producer {
	if interval <= 0 {
		interval = 120 * time.Second
	}
	return &Producer{
		store:    store,
		interval: interval,
		stopCh:   make(chan struct{}),
		maxTxs:   100,
	}
}

func (p *Producer) SetP2P(n *p2p.Node)             { p.p2pNode = n }
func (p *Producer) SetMempool(m *mempool.Mempool)   { p.pool = m }
func (p *Producer) SetLeader(l *LeaderSchedule)     { p.leader = l }
func (p *Producer) SetMaxTxsPerBlock(n int) {
	if n > 0 {
		p.maxTxs = n
	}
}

// NotifyExternalProposal should be called from handlePropose after a
// successful seal so the auto-producer yields for one cycle.
func (p *Producer) NotifyExternalProposal() {
	p.mu.Lock()
	p.lastExternalProposal = time.Now()
	p.mu.Unlock()
}

func (p *Producer) Start() {
	p.wg.Add(1)
	go p.loop()
	log.Info().Dur("interval", p.interval).Msg("auto block producer started")
}

func (p *Producer) Stop() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
	p.wg.Wait()
	log.Info().Msg("auto block producer stopped")
}

func (p *Producer) loop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	time.Sleep(2 * time.Second)
	p.trySeal("boot")

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.trySeal("tick")
		}
	}
}

func (p *Producer) trySeal(reason string) {
	p.mu.Lock()
	recent := time.Since(p.lastExternalProposal) < p.interval
	p.mu.Unlock()
	if recent {
		log.Debug().Str("reason", reason).Msg("skip auto-seal — external proposal just landed")
		return
	}

	latest, err := p.store.GetLatestHeight()
	if err != nil {
		log.Warn().Err(err).Msg("auto-seal: get height failed")
		return
	}
	nextHeight := latest + 1

	// Leader election: only produce if we are the scheduled leader
	if p.leader != nil && !p.leader.IsLocalLeader(nextHeight) {
		leader := p.leader.LeaderForHeight(nextHeight)
		log.Debug().
			Uint64("height", nextHeight).
			Str("leader", leader).
			Str("reason", reason).
			Msg("skip auto-seal — not local leader this height")
		return
	}

	sealerKey := os.Getenv("CHAIN_SIGNING_PRIVATE_KEY")
	proposer := os.Getenv("CHAIN_SIGNING_ADDRESS")
	if proposer == "" && sealerKey != "" {
		if addr, err := crypto.AddressFromPrivateKey(sealerKey); err == nil {
			proposer = addr
		}
	}
	if proposer == "" {
		proposer = "0x0000000000000000000000000000000000000001"
	}

	// Pull txs from mempool (CometBFT-style)
	var txs []*tx.Transaction
	if p.pool != nil {
		txs = p.pool.Take(p.maxTxs)
	}

	result, err := SealBlockWithTxs(p.store, sealerKey, proposer, txs)
	if err != nil {
		// put txs back if seal failed
		if p.pool != nil {
			for _, t := range txs {
				_, _ = p.pool.Add(t)
			}
		}
		log.Warn().Err(err).Str("reason", reason).Msg("auto-seal failed")
		return
	}

	log.Info().
		Uint64("height", result.Header.Height).
		Str("hash", result.Header.Hash).
		Str("proposer", result.Header.Proposer).
		Int("txs", result.Header.TxCount).
		Str("reason", reason).
		Msg("block auto-sealed")

	if p.p2pNode != nil {
		_ = p.p2pNode.PublishBlock(p2p.BlockAnnounce{
			Height:    result.Header.Height,
			Hash:      result.Header.Hash,
			PrevHash:  result.Header.PrevHash,
			Timestamp: result.Header.Timestamp,
			Proposer:  result.Header.Proposer,
			Signature: result.Header.Signature,
			TxHashes:  result.Header.TxHashes,
			Txs:       result.Txs,
		})
	}
}

// SealBlockWithTxs applies txs to state, then seals a block containing them.
func SealBlockWithTxs(store *state.Store, sealerPrivKey, proposer string, txs []*tx.Transaction) (*SealResult, error) {
	latest, err := store.GetLatestHeight()
	if err != nil {
		return nil, err
	}
	height := latest + 1

	var prevHash string
	if latest == 0 {
		prevHash = "GENESIS"
	} else {
		prev, err := store.GetBlock(latest)
		if err != nil {
			return nil, fmt.Errorf("load prev block: %w", err)
		}
		prevHash = prev.Hash
	}

	txHashes := make([]string, 0, len(txs))
	applied := make([]*tx.Transaction, 0, len(txs))
	for _, t := range txs {
		if err := store.ApplyTx(t); err != nil {
			log.Warn().Err(err).Str("tx", t.Hash()).Msg("skip tx during seal")
			continue
		}
		txHashes = append(txHashes, "0x"+t.Hash())
		applied = append(applied, t)
	}

	ts := time.Now().UnixMilli()
	merkle := RecomputeMerkleRoot(txHashes)
	blockHash := RecomputeBlockHash(height, prevHash, merkle, ts, proposer)

	var sealerSig string
	if sealerPrivKey != "" {
		sealMsg := crypto.BuildChainSigningMessage(height, blockHash)
		sealerSig, err = crypto.SignEIP191(sealMsg, sealerPrivKey)
		if err != nil {
			return nil, fmt.Errorf("sealer sign failed: %w", err)
		}
	}

	stateRoot, err := store.ComputeStateRoot()
	if err != nil {
		stateRoot = ""
	}

	header := state.BlockHeader{
		Height:    height,
		Hash:      blockHash,
		PrevHash:  prevHash,
		StateRoot: stateRoot,
		TxHashes:  txHashes,
		Timestamp: ts,
		Proposer:  proposer,
		TxCount:   len(txHashes),
		Signature: sealerSig,
	}

	if err := store.SaveBlock(header); err != nil {
		return nil, err
	}
	return &SealResult{Header: header, SealerSig: sealerSig, Txs: applied}, nil
}

// SealEmptyBlock keeps the old helper for tests / manual calls.
func SealEmptyBlock(store *state.Store, sealerPrivKey, proposer string) (*SealResult, error) {
	return SealBlockWithTxs(store, sealerPrivKey, proposer, nil)
}
