package consensus

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/crypto"
	"github.com/eastchain/east-validator/internal/mempool"
	"github.com/eastchain/east-validator/internal/p2p"
	"github.com/eastchain/east-validator/internal/state"
	"github.com/eastchain/east-validator/internal/tx"
)

// RoundStep is the Tendermint-inspired state machine step.
type RoundStep int

const (
	StepNewHeight RoundStep = iota
	StepPropose
	StepPrevote
	StepPrecommit
	StepCommit
)

func (s RoundStep) String() string {
	switch s {
	case StepNewHeight:
		return "NewHeight"
	case StepPropose:
		return "Propose"
	case StepPrevote:
		return "Prevote"
	case StepPrecommit:
		return "Precommit"
	case StepCommit:
		return "Commit"
	default:
		return fmt.Sprintf("Step(%d)", s)
	}
}

// BFTConfig controls timeouts (milliseconds base; each round multiplies).
type BFTConfig struct {
	// ProposeTimeout is how long non-leaders wait for a proposal before prevote-nil.
	ProposeTimeout time.Duration
	// PrevoteTimeout is how long to collect prevotes before deciding.
	PrevoteTimeout time.Duration
	// PrecommitTimeout is how long to collect precommits before next round.
	PrecommitTimeout time.Duration
	// RoundTimeoutDelta added per round number (linear backoff).
	RoundTimeoutDelta time.Duration
	// MinBlockInterval is the minimum wall-clock time between committed heights.
	// Prevents solo/fast-quorum from sealing every few hundred ms.
	// Defaults to BLOCK_INTERVAL_SEC (typically 120s) when wired from main.
	MinBlockInterval time.Duration
	// Enabled turns on full BFT. When false, falls back to immediate seal (legacy).
	Enabled bool
}

func DefaultBFTConfig() BFTConfig {
	return BFTConfig{
		ProposeTimeout:    5 * time.Second,
		PrevoteTimeout:    5 * time.Second,
		PrecommitTimeout:  5 * time.Second,
		RoundTimeoutDelta: 1 * time.Second,
		MinBlockInterval:  120 * time.Second,
		Enabled:           true,
	}
}

// BFTEngine runs a simplified Tendermint consensus loop for one validator.
//
// Protocol (equal-weight validators, Phase-1):
//  1. Leader for (height, round) builds + signs a Proposal, broadcasts it.
//  2. Every validator prevotes for the proposal (or NIL on timeout / invalid).
//  3. On +2/3 prevotes for the same hash → precommit that hash (else NIL).
//  4. On +2/3 precommits for the same hash → COMMIT: apply txs + SaveBlock.
//  5. Otherwise increment round and restart from Propose.
//
// Solo / single-validator (n < 2): skips voting and seals immediately
// (same behaviour as the old Producer path).
type BFTEngine struct {
	store  *state.Store
	leader *LeaderSchedule
	pool   *mempool.Mempool
	p2p    *p2p.Node
	cfg    BFTConfig
	maxTxs int

	localAddr string
	privKey   string

	mu sync.Mutex

	// Current consensus state
	height      uint64
	round       int32
	step        RoundStep
	proposal    *Proposal
	lockedHash  string
	lockedRound int32
	validHash   string
	validRound  int32

	// Votes indexed by voter address (lowercase)
	prevotes   map[string]*Vote
	precommits map[string]*Vote

	// Pending block material for the current proposal (leader only)
	pendingTxs []*tx.Transaction

	// Double-sign detection: voter → last signed (height, round, type, hash)
	lastVotes map[string]voteKey

	stopCh       chan struct{}
	lastCommitAt time.Time
	wg     sync.WaitGroup

	// Callbacks
	onCommit func(header state.BlockHeader, txs []*tx.Transaction)
}

type voteKey struct {
	height uint64
	round  int32
	vtype  VoteType
	hash   string
}

func NewBFTEngine(store *state.Store, leader *LeaderSchedule, cfg BFTConfig) *BFTEngine {
	local := os.Getenv("CHAIN_SIGNING_ADDRESS")
	if local == "" {
		if k := os.Getenv("CHAIN_SIGNING_PRIVATE_KEY"); k != "" {
			if addr, err := crypto.AddressFromPrivateKey(k); err == nil {
				local = addr
			}
		}
	}
	return &BFTEngine{
		store:       store,
		leader:      leader,
		cfg:         cfg,
		maxTxs:      100,
		localAddr:   strings.ToLower(strings.TrimSpace(local)),
		privKey:     os.Getenv("CHAIN_SIGNING_PRIVATE_KEY"),
		prevotes:    make(map[string]*Vote),
		precommits:  make(map[string]*Vote),
		lastVotes:   make(map[string]voteKey),
		lockedRound: -1,
		validRound:  -1,
		stopCh:      make(chan struct{}),
	}
}

func (e *BFTEngine) SetMempool(m *mempool.Mempool) { e.pool = m }
func (e *BFTEngine) SetP2P(n *p2p.Node)             { e.p2p = n }
func (e *BFTEngine) SetMaxTxs(n int) {
	if n > 0 {
		e.maxTxs = n
	}
}
func (e *BFTEngine) SetOnCommit(fn func(state.BlockHeader, []*tx.Transaction)) {
	e.onCommit = fn
}

func (e *BFTEngine) Start() {
	if !e.cfg.Enabled {
		log.Info().Msg("BFT engine disabled — use legacy Producer")
		return
	}
	e.wg.Add(1)
	go e.loop()
	log.Info().
		Str("local", e.localAddr).
		Dur("propose_timeout", e.cfg.ProposeTimeout).
		Msg("BFT consensus engine started")
}

func (e *BFTEngine) Stop() {
	select {
	case <-e.stopCh:
	default:
		close(e.stopCh)
	}
	e.wg.Wait()
	log.Info().Msg("BFT consensus engine stopped")
}

// HandleProposal is called when a BFT proposal arrives via P2P or HTTP.
func (e *BFTEngine) HandleProposal(p *Proposal) {
	if err := VerifyProposalBFT(p); err != nil {
		log.Warn().Err(err).Uint64("height", p.Height).Msg("BFT: reject proposal — bad signature")
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if p.Height != e.height {
		log.Debug().Uint64("got", p.Height).Uint64("want", e.height).Msg("BFT: proposal for wrong height")
		return
	}
	if p.Round != e.round {
		log.Debug().Int32("got", p.Round).Int32("want", e.round).Msg("BFT: proposal for wrong round")
		return
	}
	if e.step > StepPropose {
		return // already moved on
	}

	// Must be the scheduled leader for this height
	expected := ""
	if e.leader != nil {
		expected = e.leader.LeaderForHeight(p.Height)
	}
	if expected != "" && !strings.EqualFold(p.Proposer, expected) {
		log.Warn().
			Str("proposer", p.Proposer).
			Str("expected", expected).
			Uint64("height", p.Height).
			Msg("BFT: proposal from non-leader — rejected")
		return
	}

	// Validate against local chain tip
	if err := e.validateProposalLocked(p); err != nil {
		log.Warn().Err(err).Msg("BFT: proposal failed chain validation")
		return
	}

	e.proposal = p
	e.validHash = p.BlockHash
	e.validRound = p.Round
	// Non-leader: keep full tx bodies so commit applies the same state.
	if len(p.Txs) > 0 {
		e.pendingTxs = p.Txs
	}
	log.Info().
		Uint64("height", p.Height).
		Int32("round", p.Round).
		Str("hash", p.BlockHash[:min(16, len(p.BlockHash))]+"...").
		Str("proposer", p.Proposer).
		Int("txs", len(p.Txs)).
		Msg("BFT: proposal accepted")

	// Move to prevote immediately if we were waiting
	if e.step == StepPropose {
		e.enterPrevoteLocked()
	}
}

// HandleVote is called when a prevote/precommit arrives via P2P or HTTP.
func (e *BFTEngine) HandleVote(v *Vote) {
	if err := VerifyVote(v); err != nil {
		log.Warn().Err(err).Str("voter", v.Voter).Msg("BFT: reject vote — bad signature")
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Reject jailed validators immediately
	if e.store != nil {
		if jailed, _, err := e.store.IsJailed(v.Voter); err == nil && jailed {
			log.Debug().Str("voter", v.Voter).Msg("BFT: vote from jailed validator — ignored")
			return
		}
	}

	// Only accept votes from the current validator set
	if e.leader != nil {
		known := false
		for _, addr := range e.leader.Validators() {
			if strings.EqualFold(addr, v.Voter) {
				known = true
				break
			}
		}
		// Solo mode (empty set): accept own votes only
		if e.leader.Count() == 0 {
			known = strings.EqualFold(v.Voter, e.localAddr)
		}
		if !known {
			log.Debug().Str("voter", v.Voter).Msg("BFT: vote from non-validator — ignored")
			return
		}
	}

	if v.Height != e.height {
		return
	}

	// Double-sign detection
	key := fmt.Sprintf("%s|%s", v.Voter, v.Type)
	if prev, ok := e.lastVotes[key]; ok {
		if prev.height == v.Height && prev.round == v.Round && prev.hash != v.BlockHash {
			log.Error().
				Str("voter", v.Voter).
				Str("type", string(v.Type)).
				Uint64("height", v.Height).
				Int32("round", v.Round).
				Str("hash_a", prev.hash).
				Str("hash_b", v.BlockHash).
				Msg("BFT: EQUIVOCATION DETECTED — double-sign evidence")
			if e.store != nil {
				_ = JailOnEquivocation(e.store, v.Voter, v.Height, v.Round, prev.hash, v.BlockHash)
			}
		}
	}
	e.lastVotes[key] = voteKey{height: v.Height, round: v.Round, vtype: v.Type, hash: v.BlockHash}

	switch v.Type {
	case VotePrevote:
		if _, exists := e.prevotes[strings.ToLower(v.Voter)]; exists {
			return // already have this voter's prevote for this round
		}
		if v.Round != e.round {
			return
		}
		e.prevotes[strings.ToLower(v.Voter)] = v
		log.Debug().Str("voter", v.Voter).Str("hash", shortHash(v.BlockHash)).Int("total", len(e.prevotes)).Msg("BFT: prevote recorded")
		e.tryFinalizePrevotesLocked()

	case VotePrecommit:
		if _, exists := e.precommits[strings.ToLower(v.Voter)]; exists {
			return
		}
		if v.Round != e.round {
			return
		}
		e.precommits[strings.ToLower(v.Voter)] = v
		log.Debug().Str("voter", v.Voter).Str("hash", shortHash(v.BlockHash)).Int("total", len(e.precommits)).Msg("BFT: precommit recorded")
		e.tryFinalizePrecommitsLocked()
	}
}

func (e *BFTEngine) loop() {
	defer e.wg.Done()

	// Align to chain tip
	latest, err := e.store.GetLatestHeight()
	if err != nil {
		log.Error().Err(err).Msg("BFT: cannot read latest height")
		return
	}
	e.mu.Lock()
	e.height = latest + 1
	e.round = 0
	e.step = StepNewHeight
	e.mu.Unlock()

	time.Sleep(2 * time.Second) // let P2P settle

	for {
		select {
		case <-e.stopCh:
			return
		default:
		}

		e.mu.Lock()
		h, r, step := e.height, e.round, e.step
		e.mu.Unlock()

		switch step {
		case StepNewHeight, StepPropose:
			e.runPropose(h, r)
		case StepPrevote:
			e.runPrevote(h, r)
		case StepPrecommit:
			e.runPrecommit(h, r)
		case StepCommit:
			// commit already applied inside tryFinalizePrecommitsLocked
			e.waitBlockInterval()
		}

		// Small yield so we don't busy-spin
		time.Sleep(50 * time.Millisecond)
	}
}

// waitBlockInterval sleeps until MinBlockInterval has elapsed since last commit.
// Solo BFT reaches +2/3 instantly; without this, empty blocks would seal every ~100ms.
//
// When the mempool has pending txs, use a short interval (default 3s, env
// BFT_TX_MIN_INTERVAL_MS) so send/stake are not stuck behind the empty-block
// cadence (e.g. 180s). Empty mempool keeps the full MinBlockInterval.
func (e *BFTEngine) waitBlockInterval() {
	interval := e.cfg.MinBlockInterval
	if interval <= 0 {
		interval = 120 * time.Second
	}

	mempoolN := 0
	if e.pool != nil {
		mempoolN = e.pool.Size()
	}
	// Fast path: pending user txs → do not wait full empty-block interval
	if mempoolN > 0 {
		fast := 3 * time.Second
		if v := os.Getenv("BFT_TX_MIN_INTERVAL_MS"); v != "" {
			if n, err := time.ParseDuration(v + "ms"); err == nil && n >= 500*time.Millisecond {
				fast = n
			}
		}
		if fast < interval {
			interval = fast
		}
	}

	e.mu.Lock()
	last := e.lastCommitAt
	e.mu.Unlock()
	if last.IsZero() {
		return
	}
	elapsed := time.Since(last)
	if elapsed >= interval {
		return
	}
	wait := interval - elapsed
	log.Debug().
		Dur("wait", wait).
		Dur("interval", interval).
		Int("mempool", mempoolN).
		Msg("BFT: pacing next height to MinBlockInterval")
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-e.stopCh:
		return
	case <-timer.C:
		return
	}
}

func (e *BFTEngine) runPropose(height uint64, round int32) {
	// Pace block production (multi-validator path was skipping StepCommit wait).
	e.waitBlockInterval()

	e.mu.Lock()
	if e.step != StepNewHeight && e.step != StepPropose {
		e.mu.Unlock()
		return
	}
	e.step = StepPropose
	e.proposal = nil
	e.prevotes = make(map[string]*Vote)
	e.precommits = make(map[string]*Vote)
	e.pendingTxs = nil
	n := 0
	if e.leader != nil {
		n = len(e.leader.Validators())
		if n == 0 {
			// eligible path
			n = 1 // treat solo as n=1
		}
	}
	isLeader := e.leader == nil || e.leader.IsLocalLeader(height)
	e.mu.Unlock()

	// Solo / single validator: seal immediately without voting
	if n <= 1 || (e.leader != nil && e.leader.Count() <= 1) {
		e.soloSeal(height)
		return
	}

	if isLeader && e.privKey != "" {
		if err := e.buildAndBroadcastProposal(height, round); err != nil {
			log.Warn().Err(err).Uint64("height", height).Int32("round", round).Msg("BFT: failed to build proposal")
		}
	}

	// Wait for proposal or timeout
	timeout := e.cfg.ProposeTimeout + time.Duration(round)*e.cfg.RoundTimeoutDelta
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-e.stopCh:
			return
		default:
		}
		e.mu.Lock()
		has := e.proposal != nil
		e.mu.Unlock()
		if has {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	e.mu.Lock()
	if e.step == StepPropose {
		e.enterPrevoteLocked()
	}
	e.mu.Unlock()
}

func (e *BFTEngine) buildAndBroadcastProposal(height uint64, round int32) error {
	latest, err := e.store.GetLatestHeight()
	if err != nil {
		return err
	}
	if height != latest+1 {
		return fmt.Errorf("height drift: want %d have tip %d", height, latest)
	}

	var prevHash string
	if latest == 0 {
		prevHash = "GENESIS"
	} else {
		prev, err := e.store.GetBlock(latest)
		if err != nil {
			return err
		}
		prevHash = prev.Hash
	}

	var txs []*tx.Transaction
	if e.pool != nil {
		txs = e.pool.Take(e.maxTxs)
	}
	txHashes := make([]string, 0, len(txs))
	for _, t := range txs {
		txHashes = append(txHashes, "0x"+t.Hash())
	}
	ts := time.Now().UnixMilli()
	merkle := RecomputeMerkleRoot(txHashes)
	blockHash := RecomputeBlockHash(height, prevHash, merkle, ts, e.localAddr)

	p := &Proposal{
		Height:     height,
		Round:      round,
		BlockHash:  blockHash,
		PrevHash:   prevHash,
		MerkleRoot: merkle,
		Timestamp:  ts,
		Proposer:   e.localAddr,
		TxHashes:   txHashes,
		Txs:        txs,
		POLRound:   -1,
	}
	if err := SignProposalBFT(p, e.privKey); err != nil {
		// return txs to mempool
		if e.pool != nil {
			for _, t := range txs {
				_, _ = e.pool.Add(t)
			}
		}
		return err
	}

	e.mu.Lock()
	e.proposal = p
	e.pendingTxs = txs
	e.validHash = blockHash
	e.validRound = round
	e.mu.Unlock()

	if e.p2p != nil {
		_ = e.p2p.PublishConsensus(p2p.ConsensusMsg{
			Kind:     p2p.ConsensusKindProposal,
			Proposal: proposalToP2P(p),
		})
	}

	log.Info().
		Uint64("height", height).
		Int32("round", round).
		Str("hash", shortHash(blockHash)).
		Int("txs", len(txHashes)).
		Msg("BFT: proposal broadcast")
	return nil
}

func (e *BFTEngine) enterPrevoteLocked() {
	e.step = StepPrevote
	hash := ""
	if e.proposal != nil {
		// Tendermint: if locked, only prevote locked hash
		if e.lockedRound >= 0 && e.lockedHash != "" {
			hash = e.lockedHash
		} else {
			hash = e.proposal.BlockHash
		}
	}
	e.broadcastVoteLocked(VotePrevote, hash)
}

func (e *BFTEngine) runPrevote(height uint64, round int32) {
	timeout := e.cfg.PrevoteTimeout + time.Duration(round)*e.cfg.RoundTimeoutDelta
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-e.stopCh:
			return
		default:
		}
		e.mu.Lock()
		done := e.step != StepPrevote
		e.mu.Unlock()
		if done {
			return
		}
		// Also try finalize in case votes arrived
		e.mu.Lock()
		e.tryFinalizePrevotesLocked()
		e.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	// Timeout → precommit nil if still in prevote
	e.mu.Lock()
	if e.step == StepPrevote {
		e.step = StepPrecommit
		e.broadcastVoteLocked(VotePrecommit, "") // nil
	}
	e.mu.Unlock()
}

func (e *BFTEngine) tryFinalizePrevotesLocked() {
	if e.step != StepPrevote {
		return
	}
	n := e.validatorCountLocked()
	q := QuorumSize(n)
	if q == 0 {
		return
	}

	// Count prevotes by hash
	counts := map[string]int{}
	for _, v := range e.prevotes {
		counts[v.BlockHash]++
	}
	for hash, c := range counts {
		if c >= q {
			if hash != "" {
				// +2/3 for a real block → lock and precommit it
				e.lockedHash = hash
				e.lockedRound = e.round
				e.validHash = hash
				e.validRound = e.round
			}
			e.step = StepPrecommit
			e.broadcastVoteLocked(VotePrecommit, hash)
			return
		}
	}
	// +2/3 nil?
	if counts[""] >= q {
		e.step = StepPrecommit
		e.broadcastVoteLocked(VotePrecommit, "")
	}
}

func (e *BFTEngine) runPrecommit(height uint64, round int32) {
	timeout := e.cfg.PrecommitTimeout + time.Duration(round)*e.cfg.RoundTimeoutDelta
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-e.stopCh:
			return
		default:
		}
		e.mu.Lock()
		done := e.step != StepPrecommit
		e.mu.Unlock()
		if done {
			return
		}
		e.mu.Lock()
		e.tryFinalizePrecommitsLocked()
		e.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	// Timeout → next round
	e.mu.Lock()
	if e.step == StepPrecommit {
		e.round++
		e.step = StepNewHeight
		e.proposal = nil
		e.prevotes = make(map[string]*Vote)
		e.precommits = make(map[string]*Vote)
		log.Info().Uint64("height", e.height).Int32("round", e.round).Msg("BFT: round timeout — advancing round")
	}
	e.mu.Unlock()
}

func (e *BFTEngine) tryFinalizePrecommitsLocked() {
	if e.step != StepPrecommit {
		return
	}
	n := e.validatorCountLocked()
	q := QuorumSize(n)
	if q == 0 {
		return
	}

	counts := map[string]int{}
	var commitVotes []Vote
	for _, v := range e.precommits {
		counts[v.BlockHash]++
	}
	for hash, c := range counts {
		if c >= q && hash != "" {
			// Collect the precommits that form the certificate
			for _, v := range e.precommits {
				if v.BlockHash == hash {
					commitVotes = append(commitVotes, *v)
				}
			}
			e.step = StepCommit
			e.commitLocked(hash, commitVotes)
			return
		}
	}
	if counts[""] >= q {
		// +2/3 nil precommit → next round
		e.round++
		e.step = StepNewHeight
		e.proposal = nil
		e.prevotes = make(map[string]*Vote)
		e.precommits = make(map[string]*Vote)
		log.Info().Uint64("height", e.height).Int32("round", e.round).Msg("BFT: +2/3 nil precommit — next round")
	}
}

func (e *BFTEngine) commitLocked(blockHash string, votes []Vote) {
	log.Info().
		Uint64("height", e.height).
		Int32("round", e.round).
		Str("hash", shortHash(blockHash)).
		Int("precommits", len(votes)).
		Msg("BFT: COMMIT")

	// Apply block
	var header state.BlockHeader
	var applied []*tx.Transaction

	if e.proposal != nil && e.proposal.BlockHash == blockHash {
		// We have the full proposal (we were leader or received it)
		txs := e.pendingTxs
		// If we were not leader, pendingTxs is nil — apply from empty / future: need tx body gossip
		// Phase-1: leader applies its own txs; non-leaders apply empty state transition matching hashes
		// (full tx body relay for non-leader is via existing BlockAnnounce after commit)
		if txs == nil {
			txs = []*tx.Transaction{}
		}
		result, err := sealFromProposal(e.store, e.proposal, txs, e.privKey)
		if err != nil {
			log.Error().Err(err).Msg("BFT: commit seal failed")
			// Advance anyway to avoid stuck
			e.advanceHeightLocked()
			return
		}
		header = result.Header
		header.LastCommitRound = e.round
		header.LastCommitVotes = len(votes)
		// Re-save with commit certificate fields
		_ = e.store.SaveBlock(header)
		e.markCommitted()
		applied = result.Txs
		// Stay on StepCommit so the engine loop can also pace; advance after gossip below.
	} else {
		// Should not happen often — we committed a hash we never saw as proposal
		log.Error().Str("hash", blockHash).Msg("BFT: commit without matching proposal — skipping apply")
		e.advanceHeightLocked()
		return
	}

	// Gossip the committed block so full nodes / light path stay in sync
	if e.p2p != nil {
		_ = e.p2p.PublishBlock(p2p.BlockAnnounce{
			Height:    header.Height,
			Hash:      header.Hash,
			PrevHash:  header.PrevHash,
			Timestamp: header.Timestamp,
			Proposer:  header.Proposer,
			Signature: header.Signature,
			TxHashes:  header.TxHashes,
			Txs:       applied,
		})
		// Also publish commit certificate
		_ = e.p2p.PublishConsensus(p2p.ConsensusMsg{
			Kind: p2p.ConsensusKindCommit,
			Commit: &p2p.CommitCert{
				Height:    header.Height,
				Round:     e.round,
				BlockHash: header.Hash,
				VoteCount: len(votes),
			},
		})
	}

	if e.onCommit != nil {
		e.onCommit(header, applied)
	}

	// Release lock while sleeping so vote handlers are not blocked for the full interval.
	// Caller (tryFinalizePrecommitsLocked) holds e.mu — unlock/lock around the wait.
	e.mu.Unlock()
	e.waitBlockInterval()
	e.mu.Lock()
	e.advanceHeightLocked()
}

func (e *BFTEngine) markCommitted() {
	e.lastCommitAt = time.Now()
}

func (e *BFTEngine) advanceHeightLocked() {
	e.height++
	e.round = 0
	e.step = StepNewHeight
	e.proposal = nil
	e.pendingTxs = nil
	e.prevotes = make(map[string]*Vote)
	e.precommits = make(map[string]*Vote)
	// Keep lock across heights (Tendermint does) — clear on successful commit of matching hash
	// Simplified: clear lock after commit
	e.lockedHash = ""
	e.lockedRound = -1
	e.validHash = ""
	e.validRound = -1
}

func (e *BFTEngine) soloSeal(height uint64) {
	// Same path as old Producer.trySeal for n<=1
	e.waitBlockInterval()
	sealerKey := e.privKey
	proposer := e.localAddr
	if proposer == "" {
		proposer = "0x0000000000000000000000000000000000000001"
	}
	var txs []*tx.Transaction
	if e.pool != nil {
		txs = e.pool.Take(e.maxTxs)
	}
	result, err := SealBlockWithTxs(e.store, sealerKey, proposer, txs)
	if err != nil {
		if e.pool != nil {
			for _, t := range txs {
				_, _ = e.pool.Add(t)
			}
		}
		log.Warn().Err(err).Uint64("height", height).Msg("BFT solo-seal failed")
		time.Sleep(time.Second)
		return
	}
	e.mu.Lock()
	e.markCommitted()
	e.mu.Unlock()
	log.Info().
		Uint64("height", result.Header.Height).
		Str("hash", shortHash(result.Header.Hash)).
		Int("txs", result.Header.TxCount).
		Dur("next_in", e.cfg.MinBlockInterval).
		Msg("BFT solo: block sealed")

	if e.p2p != nil {
		_ = e.p2p.PublishBlock(p2p.BlockAnnounce{
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
	if e.onCommit != nil {
		e.onCommit(result.Header, result.Txs)
	}

	e.mu.Lock()
	e.advanceHeightLocked()
	e.mu.Unlock()
}

func (e *BFTEngine) broadcastVoteLocked(vtype VoteType, blockHash string) {
	if e.privKey == "" || e.localAddr == "" {
		return
	}
	v, err := SignVote(vtype, e.height, e.round, blockHash, e.localAddr, e.privKey, time.Now().UnixMilli())
	if err != nil {
		log.Warn().Err(err).Str("type", string(vtype)).Msg("BFT: sign vote failed")
		return
	}
	// Record own vote
	switch vtype {
	case VotePrevote:
		e.prevotes[e.localAddr] = v
	case VotePrecommit:
		e.precommits[e.localAddr] = v
	}
	if e.p2p != nil {
		_ = e.p2p.PublishConsensus(p2p.ConsensusMsg{
			Kind: p2p.ConsensusKindVote,
			Vote: voteToP2P(v),
		})
	}
	log.Debug().
		Str("type", string(vtype)).
		Uint64("height", e.height).
		Int32("round", e.round).
		Str("hash", shortHash(blockHash)).
		Msg("BFT: vote broadcast")
}

func (e *BFTEngine) validateProposalLocked(p *Proposal) error {
	latest, err := e.store.GetLatestHeight()
	if err != nil {
		return err
	}
	if p.Height != latest+1 {
		return fmt.Errorf("proposal height %d != tip+1 %d", p.Height, latest+1)
	}
	var expectedPrev string
	if latest == 0 {
		expectedPrev = "GENESIS"
	} else {
		prev, err := e.store.GetBlock(latest)
		if err != nil {
			return err
		}
		expectedPrev = prev.Hash
	}
	if p.PrevHash != expectedPrev {
		return fmt.Errorf("prev_hash mismatch")
	}
	// Recompute hash
	recomputed := RecomputeBlockHash(p.Height, p.PrevHash, p.MerkleRoot, p.Timestamp, p.Proposer)
	if recomputed != p.BlockHash {
		return fmt.Errorf("block hash mismatch: got %s recomputed %s", p.BlockHash, recomputed)
	}
	return nil
}

func (e *BFTEngine) validatorCountLocked() int {
	if e.leader == nil {
		return 1
	}
	n := e.leader.Count()
	if n == 0 {
		return 1
	}
	return n
}

// Stats for /health and /consensus/status
func (e *BFTEngine) Stats() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return map[string]any{
		"enabled":        e.cfg.Enabled,
		"height":         e.height,
		"round":          e.round,
		"step":           e.step.String(),
		"has_proposal":   e.proposal != nil,
		"prevote_count":  len(e.prevotes),
		"precommit_count": len(e.precommits),
		"locked_round":   e.lockedRound,
		"locked_hash":    shortHash(e.lockedHash),
		"local_address":  e.localAddr,
		"quorum":         QuorumSize(e.validatorCountLocked()),
		"validator_n":    e.validatorCountLocked(),
	}
}

// --- helpers ---

func shortHash(h string) string {
	if h == "" {
		return "NIL"
	}
	if len(h) <= 16 {
		return h
	}
	return h[:16] + "..."
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// sealFromProposal applies txs and persists header using the proposal's
// pre-agreed hash/timestamp so all validators store the same header.
func sealFromProposal(store *state.Store, p *Proposal, txs []*tx.Transaction, sealerKey string) (*SealResult, error) {
	applied := make([]*tx.Transaction, 0, len(txs))
	txHashes := make([]string, 0, len(txs))
	for _, t := range txs {
		if err := store.ApplyTx(t); err != nil {
			log.Warn().Err(err).Str("tx", t.Hash()).Msg("BFT commit: skip tx")
			continue
		}
		txHashes = append(txHashes, "0x"+t.Hash())
		applied = append(applied, t)
	}
	// Prefer proposal's declared hashes if we applied a subset
	if len(p.TxHashes) > 0 && len(txHashes) != len(p.TxHashes) {
		log.Warn().Int("applied", len(txHashes)).Int("proposed", len(p.TxHashes)).Msg("BFT commit: tx count mismatch")
	}

	var sealerSig string
	if sealerKey != "" {
		msg := crypto.BuildChainSigningMessage(p.Height, p.BlockHash)
		sig, err := crypto.SignEIP191(msg, sealerKey)
		if err != nil {
			return nil, fmt.Errorf("sealer sign: %w", err)
		}
		sealerSig = sig
	}

	stateRoot, _ := store.ComputeStateRoot()
	header := state.BlockHeader{
		Height:    p.Height,
		Hash:      p.BlockHash,
		PrevHash:  p.PrevHash,
		StateRoot: stateRoot,
		TxHashes:  p.TxHashes,
		Timestamp: p.Timestamp,
		Proposer:  p.Proposer,
		TxCount:   len(p.TxHashes),
		Signature: sealerSig,
	}
	if err := store.SaveBlock(header); err != nil {
		return nil, err
	}
	return &SealResult{Header: header, SealerSig: sealerSig, Txs: applied}, nil
}

func proposalToP2P(p *Proposal) *p2p.BFTProposal {
	return &p2p.BFTProposal{
		Height:     p.Height,
		Round:      p.Round,
		BlockHash:  p.BlockHash,
		PrevHash:   p.PrevHash,
		MerkleRoot: p.MerkleRoot,
		Timestamp:  p.Timestamp,
		Proposer:   p.Proposer,
		TxHashes:   p.TxHashes,
		Txs:        p.Txs,
		POLRound:   p.POLRound,
		Signature:  p.Signature,
	}
}

func voteToP2P(v *Vote) *p2p.BFTVote {
	return &p2p.BFTVote{
		Type:      string(v.Type),
		Height:    v.Height,
		Round:     v.Round,
		BlockHash: v.BlockHash,
		Voter:     v.Voter,
		Timestamp: v.Timestamp,
		Signature: v.Signature,
	}
}

// ProposalFromP2P converts a wire proposal into the consensus type.
func ProposalFromP2P(p *p2p.BFTProposal) *Proposal {
	if p == nil {
		return nil
	}
	return &Proposal{
		Height:     p.Height,
		Round:      p.Round,
		BlockHash:  p.BlockHash,
		PrevHash:   p.PrevHash,
		MerkleRoot: p.MerkleRoot,
		Timestamp:  p.Timestamp,
		Proposer:   p.Proposer,
		TxHashes:   p.TxHashes,
		Txs:        p.Txs,
		POLRound:   p.POLRound,
		Signature:  p.Signature,
	}
}

// VoteFromP2P converts a wire vote into the consensus type.
func VoteFromP2P(v *p2p.BFTVote) *Vote {
	if v == nil {
		return nil
	}
	return &Vote{
		Type:      VoteType(v.Type),
		Height:    v.Height,
		Round:     v.Round,
		BlockHash: v.BlockHash,
		Voter:     v.Voter,
		Timestamp: v.Timestamp,
		Signature: v.Signature,
	}
}
