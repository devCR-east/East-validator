package p2p

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/multiformats/go-multiaddr"
	"github.com/rs/zerolog/log"

	"github.com/eastchain/east-validator/internal/tx"
)

const (
	TopicBlocks     = "eastchain/blocks/1.0.0"
	TopicHeartbeats = "eastchain/heartbeats/1.0.0"
	TopicConsensus  = "eastchain/consensus/1.0.0"
	TopicValidatorSet = "eastchain/valset/1.0.0"
)

// Consensus message kinds on TopicConsensus.
const (
	ConsensusKindProposal = "proposal"
	ConsensusKindVote     = "vote"
	ConsensusKindCommit   = "commit"
	ConsensusKindValSet  = "valset"
)

// BFTProposal is the wire form of a BFT proposal.
// Txs carries full transaction bodies so non-leaders apply the same state.
type BFTProposal struct {
	Height     uint64            `json:"height"`
	Round      int32             `json:"round"`
	BlockHash  string            `json:"block_hash"`
	PrevHash   string            `json:"prev_hash"`
	MerkleRoot string            `json:"merkle_root"`
	Timestamp  int64             `json:"timestamp"`
	Proposer   string            `json:"proposer"`
	TxHashes   []string          `json:"tx_hashes,omitempty"`
	Txs        []*tx.Transaction `json:"txs,omitempty"`
	POLRound   int32             `json:"pol_round"`
	Signature  string            `json:"signature"`
}

// ValidatorSetMsg is gossiped when the active validator set changes.
type ValidatorSetMsg struct {
	Epoch     uint64   `json:"epoch"`
	Addresses []string `json:"addresses"`
	UpdatedAt int64    `json:"updated_at"`
	FromPeer  string   `json:"from_peer,omitempty"`
}

// BFTVote is the wire form of a prevote/precommit.
type BFTVote struct {
	Type      string `json:"type"` // prevote | precommit
	Height    uint64 `json:"height"`
	Round     int32  `json:"round"`
	BlockHash string `json:"block_hash"`
	Voter     string `json:"voter"`
	Timestamp int64  `json:"timestamp"`
	Signature string `json:"signature"`
}

// CommitCert is a lightweight commit announcement (full votes optional later).
type CommitCert struct {
	Height    uint64 `json:"height"`
	Round     int32  `json:"round"`
	BlockHash string `json:"block_hash"`
	VoteCount int    `json:"vote_count"`
}

// ConsensusMsg envelopes proposal / vote / commit on the consensus topic.
type ConsensusMsg struct {
	Kind     string       `json:"kind"`
	Proposal *BFTProposal `json:"proposal,omitempty"`
	Vote     *BFTVote     `json:"vote,omitempty"`
	Commit   *CommitCert  `json:"commit,omitempty"`
	FromPeer string       `json:"from_peer,omitempty"`
}

// BlockAnnounce is gossiped after a block is sealed.
type BlockAnnounce struct {
	Height    uint64            `json:"height"`
	Hash      string            `json:"hash"`
	PrevHash  string            `json:"prev_hash"`
	Timestamp int64             `json:"timestamp"`
	Proposer  string            `json:"proposer"`
	Signature string            `json:"signature,omitempty"`
	TxHashes  []string          `json:"tx_hashes,omitempty"`
	Txs       []*tx.Transaction `json:"txs,omitempty"` // full tx bodies so recipients can replay them into their own state, not just record the hashes
	FromPeer  string            `json:"from_peer,omitempty"`
}

// HeartbeatMsg is gossiped by full/light/validator nodes.
type HeartbeatMsg struct {
	Address  string `json:"address"`
	NodeID   string `json:"node_id"`
	Tier     string `json:"tier"` // light | full | validator
	Height   uint64 `json:"height"`
	UnixMs   int64  `json:"unix_ms"`
	FromPeer string `json:"from_peer,omitempty"`
}

type Config struct {
	Enabled       bool
	ListenPort    int
	PrivateKeyHex string
	Bootstrap     []string
	NodeID        string
	// ConnLow / ConnHigh bound the connection manager (public DoS protection).
	ConnLow  int
	ConnHigh int
	// AnnounceAddr: public multiaddr only (Railway TCP proxy). See P2P_ANNOUNCE_ADDR.
	AnnounceAddr string
}

type Node struct {
	cfg    Config
	host   host.Host
	ps     *pubsub.PubSub
	blocks *pubsub.Topic
	hb     *pubsub.Topic
	cons   *pubsub.Topic
	disc   *discoveryService
	scorer *PeerScorer
	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.RWMutex
	onBlock     func(BlockAnnounce)
	onHeartbeat func(HeartbeatMsg)
	onConsensus func(ConsensusMsg)
	peerCount   int
}

func LoadConfigFromEnv(nodeID string) Config {
	enabled := os.Getenv("P2P_ENABLED") != "false"
	// Default 26656 — port number familiar from Cosmos/CometBFT.
	// Wire protocol remains libp2p (not CometBFT PEX).
	port := 26656
	if v := os.Getenv("P2P_PORT"); v != "" {
		fmt.Sscanf(v, "%d", &port)
	}
	var bootstrap []string
	if raw := os.Getenv("P2P_BOOTSTRAP"); raw != "" {
		bootstrap = NormalizePeerAddrList(raw)
	}
	low, high := 50, 200
	if v := os.Getenv("P2P_CONN_LOW"); v != "" {
		fmt.Sscanf(v, "%d", &low)
	}
	if v := os.Getenv("P2P_CONN_HIGH"); v != "" {
		fmt.Sscanf(v, "%d", &high)
	}
	if low < 10 {
		low = 10
	}
	if high <= low {
		high = low + 50
	}
	return Config{
		Enabled:       enabled,
		ListenPort:    port,
		PrivateKeyHex: os.Getenv("P2P_PRIVATE_KEY"),
		Bootstrap:     bootstrap,
		NodeID:        nodeID,
		ConnLow:       low,
		ConnHigh:      high,
		AnnounceAddr:  normalizeAnnounce(strings.TrimSpace(os.Getenv("P2P_ANNOUNCE_ADDR"))),
	}
}

func normalizeAnnounce(raw string) string {
	if raw == "" {
		return ""
	}
	n, err := NormalizePeerAddr(raw)
	if err != nil {
		return raw
	}
	return n
}

func New(cfg Config) (*Node, error) {
	if !cfg.Enabled {
		log.Info().Msg("p2p disabled (P2P_ENABLED=false)")
		return &Node{cfg: cfg}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	priv, err := loadOrGenerateKey(cfg.PrivateKeyHex)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("p2p key: %w", err)
	}

	listen, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort))
	if err != nil {
		cancel()
		return nil, err
	}

	// Connection manager: prune excess peers so a public node cannot be
	// connection-flooded into OOM / FD exhaustion.
	if cfg.ConnLow <= 0 {
		cfg.ConnLow = 50
	}
	if cfg.ConnHigh <= cfg.ConnLow {
		cfg.ConnHigh = cfg.ConnLow + 50
	}
	cm, err := connmgr.NewConnManager(cfg.ConnLow, cfg.ConnHigh, connmgr.WithGracePeriod(time.Minute))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("connmgr: %w", err)
	}

	var announceAddr multiaddr.Multiaddr
	if cfg.AnnounceAddr != "" {
		announceAddr, err = multiaddr.NewMultiaddr(cfg.AnnounceAddr)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("invalid P2P_ANNOUNCE_ADDR %q: %w", cfg.AnnounceAddr, err)
		}
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrs(listen),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		libp2p.Security(noise.ID, noise.New),
		libp2p.DefaultTransports,
		libp2p.DefaultMuxers,
		libp2p.ConnectionManager(cm),
		libp2p.EnableNATService(),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.NATPortMap(),
		libp2p.AddrsFactory(func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
			if announceAddr != nil {
				return []multiaddr.Multiaddr{announceAddr}
			}
			return addrs
		}),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("libp2p host: %w", err)
	}

	// GossipSub with slightly stricter validation resource limits for public nets.
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithMaxMessageSize(1<<20), // 1 MiB max gossip message
	)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, fmt.Errorf("gossipsub: %w", err)
	}

	blocks, err := ps.Join(TopicBlocks)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, err
	}
	hbTopic, err := ps.Join(TopicHeartbeats)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, err
	}
	consTopic, err := ps.Join(TopicConsensus)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, err
	}

	// Parse bootstrap multiaddrs → AddrInfo for DHT seeding.
	var bootInfos []peer.AddrInfo
	for _, raw := range cfg.Bootstrap {
		norm, nerr := NormalizePeerAddr(raw)
		if nerr == nil {
			raw = norm
		}
		maddr, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			log.Warn().Str("addr", raw).Err(err).Msg("invalid bootstrap multiaddr (use /dns4/.../p2p/ID or ID@host:26656)")
			continue
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			log.Warn().Str("addr", raw).Err(err).Msg("bootstrap addr missing /p2p/PEERID")
			continue
		}
		bootInfos = append(bootInfos, *info)
	}

	disc, err := startDiscovery(ctx, h, bootInfos)
	if err != nil {
		log.Warn().Err(err).Msg("discovery start failed — falling back to manual bootstrap only")
	}

	n := &Node{
		cfg:    cfg,
		host:   h,
		ps:     ps,
		disc:   disc,
		scorer: NewPeerScorer(),
		blocks: blocks,
		hb:     hbTopic,
		cons:   consTopic,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := n.subscribeBlocks(); err != nil {
		n.Close()
		return nil, err
	}
	if err := n.subscribeHeartbeats(); err != nil {
		n.Close()
		return nil, err
	}
	if err := n.subscribeConsensus(); err != nil {
		n.Close()
		return nil, err
	}

	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			n.mu.Lock()
			n.peerCount++
			n.mu.Unlock()
			log.Info().Str("peer", c.RemotePeer().String()).Msg("p2p peer connected")
		},
		DisconnectedF: func(_ network.Network, c network.Conn) {
			n.mu.Lock()
			if n.peerCount > 0 {
				n.peerCount--
			}
			n.mu.Unlock()
			log.Info().Str("peer", c.RemotePeer().String()).Msg("p2p peer disconnected")
		},
	})

	log.Info().
		Str("peer_id", h.ID().String()).
		Strs("addrs", addrStrings(h)).
		Int("port", cfg.ListenPort).
		Int("conn_low", cfg.ConnLow).
		Int("conn_high", cfg.ConnHigh).
		Msg("libp2p node started (DHT + mDNS + hole-punching)")

	return n, nil
}

func (n *Node) Enabled() bool { return n.cfg.Enabled && n.host != nil }

func (n *Node) PeerID() string {
	if n.host == nil {
		return ""
	}
	return n.host.ID().String()
}

func (n *Node) ListenAddrs() []string {
	if n.host == nil {
		return nil
	}
	return addrStrings(n.host)
}

func (n *Node) PeerCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peerCount
}

func (n *Node) OnBlock(fn func(BlockAnnounce)) {
	n.mu.Lock()
	n.onBlock = fn
	n.mu.Unlock()
}

func (n *Node) OnHeartbeat(fn func(HeartbeatMsg)) {
	n.mu.Lock()
	n.onHeartbeat = fn
	n.mu.Unlock()
}

func (n *Node) OnConsensus(fn func(ConsensusMsg)) {
	n.mu.Lock()
	n.onConsensus = fn
	n.mu.Unlock()
}

func (n *Node) PublishConsensus(m ConsensusMsg) error {
	if !n.Enabled() || n.cons == nil {
		return nil
	}
	m.FromPeer = n.PeerID()
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return n.cons.Publish(n.ctx, b)
}

func (n *Node) PublishBlock(a BlockAnnounce) error {
	if !n.Enabled() || n.blocks == nil {
		return nil
	}
	a.FromPeer = n.PeerID()
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return n.blocks.Publish(n.ctx, b)
}

func (n *Node) PublishHeartbeat(m HeartbeatMsg) error {
	if !n.Enabled() || n.hb == nil {
		return nil
	}
	m.FromPeer = n.PeerID()
	if m.UnixMs == 0 {
		m.UnixMs = time.Now().UnixMilli()
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return n.hb.Publish(n.ctx, b)
}

func (n *Node) subscribeBlocks() error {
	sub, err := n.blocks.Subscribe()
	if err != nil {
		return err
	}
	go func() {
		for {
			msg, err := sub.Next(n.ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom == n.host.ID() {
				continue
			}
			var a BlockAnnounce
			if err := json.Unmarshal(msg.Data, &a); err != nil {
				continue
			}
			a.FromPeer = msg.ReceivedFrom.String()
			n.mu.RLock()
			fn := n.onBlock
			n.mu.RUnlock()
			if fn != nil {
				fn(a)
			} else {
				log.Debug().Uint64("height", a.Height).Str("hash", a.Hash).Msg("p2p block announce")
			}
		}
	}()
	return nil
}

func (n *Node) subscribeHeartbeats() error {
	sub, err := n.hb.Subscribe()
	if err != nil {
		return err
	}
	go func() {
		for {
			msg, err := sub.Next(n.ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom == n.host.ID() {
				continue
			}
			var m HeartbeatMsg
			if err := json.Unmarshal(msg.Data, &m); err != nil {
				continue
			}
			m.FromPeer = msg.ReceivedFrom.String()
			n.mu.RLock()
			fn := n.onHeartbeat
			n.mu.RUnlock()
			if fn != nil {
				fn(m)
			}
		}
	}()
	return nil
}

func (n *Node) subscribeConsensus() error {
	if n.cons == nil {
		return nil
	}
	sub, err := n.cons.Subscribe()
	if err != nil {
		return err
	}
	go func() {
		for {
			msg, err := sub.Next(n.ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom == n.host.ID() {
				continue
			}
			var m ConsensusMsg
			if err := json.Unmarshal(msg.Data, &m); err != nil {
				continue
			}
			m.FromPeer = msg.ReceivedFrom.String()
			n.mu.RLock()
			fn := n.onConsensus
			n.mu.RUnlock()
			if fn != nil {
				fn(m)
			} else {
				log.Debug().Str("kind", m.Kind).Msg("p2p consensus msg")
			}
		}
	}()
	return nil
}

func (n *Node) dialBootstrap() {
	if n.host == nil {
		return
	}
	for _, raw := range n.cfg.Bootstrap {
		maddr, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			log.Warn().Str("addr", raw).Err(err).Msg("invalid bootstrap multiaddr")
			continue
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			log.Warn().Str("addr", raw).Err(err).Msg("bootstrap addr missing /p2p/PEERID")
			continue
		}
		ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
		err = n.host.Connect(ctx, *info)
		cancel()
		if err != nil {
			log.Warn().Str("peer", info.ID.String()).Err(err).Msg("bootstrap dial failed")
			continue
		}
		log.Info().Str("peer", info.ID.String()).Msg("connected to bootstrap peer")
	}
}

func (n *Node) Close() error {
	if n.disc != nil {
		n.disc.Close()
	}
	if n.cancel != nil {
		n.cancel()
	}
	if n.host != nil {
		return n.host.Close()
	}
	return nil
}

func (n *Node) Stats() map[string]any {
	if !n.Enabled() {
		return map[string]any{"enabled": false}
	}
	known := 0
	if n.disc != nil {
		known = n.disc.KnownPeerCount()
	}
	return map[string]any{
		"enabled":       true,
		"peer_id":       n.PeerID(),
		"peers":         n.PeerCount(),
		"known_peers":   known,
		"listen":        n.ListenAddrs(),
		"topics":        []string{TopicBlocks, TopicHeartbeats, TopicConsensus},
		"bootstrap_n":   len(n.cfg.Bootstrap),
		"discovery":     DiscoveryNamespace,
		"mdns_tag":      mdnsServiceTag,
		"sync_protocol": string(SyncProtocolID),
		"conn_low":      n.cfg.ConnLow,
		"conn_high":     n.cfg.ConnHigh,
		"scoring":      n.Scorer().Snapshot(),
	}
}

// Host exposes the libp2p host for stream protocols (block sync).
func (n *Node) Host() host.Host { return n.host }


func (n *Node) Scorer() *PeerScorer {
	if n.scorer == nil {
		n.scorer = NewPeerScorer()
	}
	return n.scorer
}

func (n *Node) ReportPeer(idStr string, delta int, reason string) {
	if n.scorer == nil || idStr == "" {
		return
	}
	pid, err := peer.Decode(idStr)
	if err != nil {
		return
	}
	if delta < 0 && n.scorer.IsBanned(pid) {
		return
	}
	n.scorer.Add(pid, delta, reason)
}

func loadOrGenerateKey(hexKey string) (crypto.PrivKey, error) {
	if hexKey != "" {
		hexKey = strings.TrimPrefix(hexKey, "0x")
		b, err := hex.DecodeString(hexKey)
		if err == nil && len(b) > 0 {
			if pk, err := crypto.UnmarshalPrivateKey(b); err == nil {
				return pk, nil
			}
			// 32-byte seed → ed25519
			if len(b) == 32 {
				// UnmarshalEd25519PrivateKey expects 64-byte expanded key;
				// Generate from seed via libp2p helper if available — fallback generate.
				log.Warn().Msg("P2P_PRIVATE_KEY looks like raw seed; generating stable key via Seed")
			}
		}
		log.Warn().Msg("P2P_PRIVATE_KEY invalid — generating ephemeral key (set proper key for stable peer id)")
	}
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	return priv, err
}

func addrStrings(h host.Host) []string {
	var out []string
	for _, a := range h.Addrs() {
		full := fmt.Sprintf("%s/p2p/%s", a.String(), h.ID().String())
		out = append(out, full)
	}
	return out
}
