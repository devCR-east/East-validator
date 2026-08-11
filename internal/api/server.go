package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/config"
	"github.com/eastchain/east-validator/internal/consensus"
	"github.com/eastchain/east-validator/internal/crypto"
	"github.com/eastchain/east-validator/internal/mempool"
	"github.com/eastchain/east-validator/internal/p2p"
	"github.com/eastchain/east-validator/internal/state"
	"github.com/eastchain/east-validator/internal/tx"
)

type Server struct {
	cfg      config.Config
	store    *state.Store
	http     *http.Server
	producer *consensus.Producer
	bft      *consensus.BFTEngine
	p2p      *p2p.Node
	pool     *mempool.Mempool
	leader   *consensus.LeaderSchedule
}

func New(cfg config.Config, store *state.Store, producer *consensus.Producer, p2pNode *p2p.Node, pool *mempool.Mempool, leader *consensus.LeaderSchedule) *Server {
	s := &Server{cfg: cfg, store: store, producer: producer, p2p: p2pNode, pool: pool, leader: leader}
	r := mux.NewRouter()

	r.HandleFunc("/health", s.handleHealth).Methods("GET")
	r.HandleFunc("/metrics", s.handleMetrics).Methods("GET")
	r.HandleFunc("/rpc", s.handleJSONRPC).Methods("POST")
	r.HandleFunc("/", s.handleJSONRPC).Methods("POST") // eth-style root RPC
	r.HandleFunc("/stats", s.handleStats).Methods("GET")
	r.HandleFunc("/account/{address}", s.handleGetAccount).Methods("GET")
	r.HandleFunc("/account/{address}/proof", s.handleAccountProof).Methods("GET")
	r.HandleFunc("/block/latest", s.handleLatestBlock).Methods("GET")
	r.HandleFunc("/block/{height}", s.handleGetBlock).Methods("GET")
	r.HandleFunc("/supply", s.handleSupply).Methods("GET")
	r.HandleFunc("/supply/{bucket}", s.handleBucket).Methods("GET")

	// Uptime / node integrity (Phase 1)
	r.HandleFunc("/heartbeat", s.handleHeartbeat).Methods("POST")
	r.HandleFunc("/uptime/{address}", s.handleUptime).Methods("GET")
	r.HandleFunc("/uptime/epoch/{epoch}", s.handleEpochScores).Methods("GET")

	// Write / consensus
	r.HandleFunc("/tx", s.auth(s.handleSubmitTx)).Methods("POST")
	r.HandleFunc("/mempool", s.handleMempoolStats).Methods("GET")
	r.HandleFunc("/mempool/txs", s.handleMempoolTxs).Methods("GET")
	r.HandleFunc("/consensus/propose", s.auth(s.handlePropose)).Methods("POST")
	r.HandleFunc("/consensus/leader", s.handleLeader).Methods("GET")
	r.HandleFunc("/consensus/status", s.handleBFTStatus).Methods("GET")
	r.HandleFunc("/validator/register", s.handleRegisterValidator).Methods("POST")
	r.HandleFunc("/admin/validators", s.auth(s.handleSetValidators)).Methods("POST")
	r.HandleFunc("/admin/validators", s.handleGetValidators).Methods("GET")
	r.HandleFunc("/admin/seed", s.auth(s.handleSeed)).Methods("POST")
	r.HandleFunc("/admin/prune", s.auth(s.handlePrune)).Methods("POST")
	r.HandleFunc("/admin/backup", s.auth(s.handleBackup)).Methods("POST")
	r.HandleFunc("/admin/restore-accounts", s.auth(s.handleRestoreAccounts)).Methods("POST")
	r.HandleFunc("/admin/snapshot", s.auth(s.handleGetSnapshot)).Methods("GET")
	r.HandleFunc("/admin/import-snapshot", s.auth(s.handleImportSnapshot)).Methods("POST")
	r.HandleFunc("/admin/jail", s.handleListJailed).Methods("GET")
	r.HandleFunc("/admin/unjail", s.auth(s.handleUnjail)).Methods("POST")

	s.http = &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      withPublicGuards(r),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// SetBFT wires the BFT engine after construction (optional).
func (s *Server) SetBFT(e *consensus.BFTEngine) {
	s.bft = e
}

func (s *Server) Start() error {
	log.Info().Str("addr", s.cfg.HTTPAddr).Msg("HTTP API listening")
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown() error { return s.http.Close() }

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.APISecret == "" {
			next(w, r)
			return
		}
		if r.Header.Get("X-API-Secret") != s.cfg.APISecret {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{
		"status":           "ok",
		"node_id":          s.cfg.NodeID,
		"name":             s.cfg.NodeName,
		"role":             "sealer",
		"chain_id":         "eastchain-1",
		"numeric_chain_id": s.store.NumericChainID(),
		"auto_produce":     s.cfg.AutoProduce,
		"block_interval_s": int(s.cfg.BlockInterval.Seconds()),
		"epoch_seconds":    s.cfg.EpochSeconds,
		"current_epoch":    state.CurrentEpochID(s.cfg.EpochSeconds),
		"p2p":              s.p2pStats(),
		"mempool":          s.mempoolStats(),
		"leader":           s.leaderStats(),
	}
	if s.bft != nil {
		out["bft"] = s.bft.Stats()
	} else {
		out["bft"] = map[string]any{"enabled": false}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleBFTStatus(w http.ResponseWriter, r *http.Request) {
	if s.bft == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false, "mode": "legacy_producer"})
		return
	}
	writeJSON(w, http.StatusOK, s.bft.Stats())
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Stats())
}

func (s *Server) handleAccountProof(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	proof, err := s.store.ProveAccount(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, proof)
}

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	acc, err := s.store.GetAccount(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, acc)
}

func (s *Server) handleLatestBlock(w http.ResponseWriter, r *http.Request) {
	height, err := s.store.GetLatestHeight()
	if err != nil || height == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"height": 0})
		return
	}
	h, err := s.store.GetBlock(height)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Server) handleGetBlock(w http.ResponseWriter, r *http.Request) {
	height, err := strconv.ParseUint(mux.Vars(r)["height"], 10, 64)
	if err != nil {
		http.Error(w, "invalid height", http.StatusBadRequest)
		return
	}
	h, err := s.store.GetBlock(height)
	if err != nil {
		http.Error(w, "block not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Server) handleSupply(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.store.ListBuckets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total_max_supply": s.store.TotalMaxSupply(),
		"buckets":          buckets,
	})
}

func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["bucket"]
	b, err := s.store.GetBucket(name)
	if err != nil {
		http.Error(w, "bucket not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) handleSubmitTx(w http.ResponseWriter, r *http.Request) {
	var t tx.Transaction
	if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := t.ValidateBasic(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Prefer mempool path (CometBFT-style). Fallback: apply immediately if no pool.
	if s.pool != nil {
		h, err := s.pool.Add(&t)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":     true,
			"txHash": h,
			"type":   t.Type,
			"status": "queued",
		})
		return
	}
	if err := s.store.ApplyTx(&t); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"txHash": t.Hash(),
		"type":   t.Type,
		"status": "applied",
	})
}

// handlePropose is disabled. Fullnode Browser's role was downgraded to
// read-only (ledger + balance reads for the RPC hub) — it no longer
// proposes blocks. All production and sealing now happens exclusively via
// this validator's own auto-producer (see consensus.Producer). Kept as a
// stub returning 410 Gone rather than removed outright, so old callers get
// a clear signal instead of a generic 404.
func (s *Server) handlePropose(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "consensus/propose is disabled — block proposals are no longer accepted; this validator produces and seals blocks on its own schedule", http.StatusGone)
}

type seedRequest struct {
	Address        string `json:"address"`
	Balance        int64  `json:"balance"`         // subunits
	Staked         int64  `json:"staked"`          // subunits
	PendingUnstake int64  `json:"pending_unstake"` // subunits
	Mode           string `json:"mode,omitempty"`  // overwrite | merge_max | balance_only
}

func (s *Server) handleSeed(w http.ResponseWriter, r *http.Request) {
	var req seedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Address == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = "overwrite"
	}
	if err := s.store.SeedAccount(req.Address, req.Balance, req.Staked, req.PendingUnstake, mode); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"address": strings.ToLower(req.Address),
		"mode":    mode,
		"seeded": map[string]int64{
			"balance":         req.Balance,
			"staked":          req.Staked,
			"pending_unstake": req.PendingUnstake,
		},
	})
}

func (s *Server) handlePrune(w http.ResponseWriter, r *http.Request) {
	if err := s.store.PruneOldBlocks(s.cfg.KeepRecentBlocks); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "keep": s.cfg.KeepRecentBlocks})
}


type heartbeatRequest struct {
	Address   string `json:"address"`
	NodeID    string `json:"node_id"`
	Tier      string `json:"tier"` // "light" | "full"
	UnixMs    int64  `json:"unix_ms"`
	Signature string `json:"signature"` // EIP-191 over BuildHeartbeatMessage(address, node_id, unix_ms)
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Address == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	if req.Signature == "" {
		http.Error(w, "signature required", http.StatusBadRequest)
		return
	}
	if req.UnixMs == 0 {
		http.Error(w, "unix_ms required", http.StatusBadRequest)
		return
	}
	// Freshness window: reject signatures older than 5 minutes so a
	// captured request/signature can't be replayed forever to keep
	// inflating an address's uptime score.
	age := time.Since(time.UnixMilli(req.UnixMs))
	if age < -5*time.Minute || age > 5*time.Minute {
		http.Error(w, "unix_ms outside 5-minute freshness window", http.StatusBadRequest)
		return
	}
	msg := crypto.BuildHeartbeatMessage(req.Address, req.NodeID, req.UnixMs)
	ok, err := crypto.VerifyEIP191(msg, req.Signature, req.Address)
	if err != nil || !ok {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	tier := state.TierLight
	if req.Tier == "full" {
		tier = state.TierFull
	}
	rec, err := s.store.RecordHeartbeat(req.Address, req.NodeID, tier, s.cfg.EpochSeconds)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.p2p != nil {
		height, _ := s.store.GetLatestHeight()
		_ = s.p2p.PublishHeartbeat(p2p.HeartbeatMsg{
			Address: req.Address,
			NodeID:  req.NodeID,
			Tier:    string(tier),
			Height:  height,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"record": rec,
		"epoch":  rec.EpochID,
	})
}

func (s *Server) p2pStats() map[string]any {
	if s.p2p == nil {
		return map[string]any{"enabled": false}
	}
	return s.p2p.Stats()
}


func (s *Server) handleUptime(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	score, err := s.store.GetUptimeScore(addr, s.cfg.EpochSeconds)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, score)
}

func (s *Server) handleEpochScores(w http.ResponseWriter, r *http.Request) {
	epoch, err := strconv.ParseInt(mux.Vars(r)["epoch"], 10, 64)
	if err != nil {
		http.Error(w, "invalid epoch", http.StatusBadRequest)
		return
	}
	limit := 100
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil {
			limit = n
		}
	}
	rows, err := s.store.ListEpochScores(epoch, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"epoch": epoch,
		"count": len(rows),
		"nodes": rows,
	})
}


func (s *Server) mempoolStats() map[string]any {
	if s.pool == nil {
		return map[string]any{"enabled": false}
	}
	st := s.pool.Stats()
	st["enabled"] = true
	return st
}

func (s *Server) leaderStats() map[string]any {
	if s.leader == nil {
		return map[string]any{"enabled": false}
	}
	h, _ := s.store.GetLatestHeight()
	return s.leader.Stats(h + 1)
}

func (s *Server) handleMempoolStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.mempoolStats())
}

func (s *Server) handleMempoolTxs(w http.ResponseWriter, r *http.Request) {
	if s.pool == nil {
		writeJSON(w, http.StatusOK, map[string]any{"txs": []any{}})
		return
	}
	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	txs := s.pool.Peek(limit)
	writeJSON(w, http.StatusOK, map[string]any{"count": len(txs), "txs": txs})
}

func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.leaderStats())
}

func (s *Server) handleGetValidators(w http.ResponseWriter, r *http.Request) {
	if s.leader == nil {
		writeJSON(w, http.StatusOK, map[string]any{"validators": []string{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"validators": s.leader.Validators()})
}

type setValidatorsRequest struct {
	Validators []string `json:"validators"`
}

type validatorRegisterRequest struct {
	Address   string `json:"address"`
	UnixMs    int64  `json:"unix_ms"`
	Signature string `json:"signature"` // EIP-191 over BuildValidatorRegisterMessage(address, unix_ms)
}

// handleRegisterValidator lets a node self-register as a block producer.
// Public (no API_SECRET) — protected instead by requiring a signature that
// proves ownership of the address, and by checking the address's live
// staked balance meets the genesis minimum before accepting it. A node
// whose stake later drops below minimum isn't removed here — it's simply
// skipped from rotation on each lookup (see LeaderSchedule.eligibleValidatorsLocked).
func (s *Server) handleRegisterValidator(w http.ResponseWriter, r *http.Request) {
	if s.leader == nil {
		http.Error(w, "leader schedule not enabled", http.StatusServiceUnavailable)
		return
	}
	var req validatorRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Address == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	if req.Signature == "" {
		http.Error(w, "signature required", http.StatusBadRequest)
		return
	}
	if req.UnixMs == 0 {
		http.Error(w, "unix_ms required", http.StatusBadRequest)
		return
	}
	age := time.Since(time.UnixMilli(req.UnixMs))
	if age < -5*time.Minute || age > 5*time.Minute {
		http.Error(w, "unix_ms outside 5-minute freshness window", http.StatusBadRequest)
		return
	}
	msg := crypto.BuildValidatorRegisterMessage(req.Address, req.UnixMs)
	ok, err := crypto.VerifyEIP191(msg, req.Signature, req.Address)
	if err != nil || !ok {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	eligible, err := s.leader.IsEligible(req.Address)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !eligible {
		http.Error(w, "address does not meet minimum validator stake", http.StatusForbidden)
		return
	}

	s.leader.AddValidator(req.Address)
	h, _ := s.store.GetLatestHeight()
	log.Info().Str("address", req.Address).Msg("validator registered")
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"leader": s.leader.Stats(h + 1),
	})
}

func (s *Server) handleSetValidators(w http.ResponseWriter, r *http.Request) {
	if s.leader == nil {
		http.Error(w, "leader schedule not enabled", http.StatusServiceUnavailable)
		return
	}
	var req setValidatorsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	s.leader.SetValidators(req.Validators)
	h, _ := s.store.GetLatestHeight()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"leader": s.leader.Stats(h + 1),
	})
}


func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	path, err := s.store.ExportBackup(500)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "path": path})
}

func (s *Server) handleRestoreAccounts(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path  string `json:"path"`
		Force bool   `json:"force"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Path == "" {
		http.Error(w, "path required", http.StatusBadRequest)
		return
	}
	n, err := s.store.RestoreAccountsFromSnapshot(req.Path, req.Force)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "accounts_applied": n})
}

func (s *Server) handleListJailed(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListJailed(200)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jailed": list})
}

func (s *Server) handleUnjail(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Address == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	if err := s.store.UnjailValidator(req.Address); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "address": req.Address})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
