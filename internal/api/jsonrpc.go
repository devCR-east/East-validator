package api

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/eastchain/east-validator/internal/tx"
)

// Minimal Ethereum-JSON-RPC surface so wallets/tools can read chain state.
// Methods: eth_chainId, eth_blockNumber, eth_getBalance, eth_getBlockByNumber,
//          eth_getBlockByHash (height scan limited), net_version, eth_gasPrice

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   *rpcErr `json:"error,omitempty"`
}

type rpcErr struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *Server) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeRPC(w, req.ID, nil, &rpcErr{Code: -32700, Message: "parse error"})
		return
	}
	result, err := s.dispatchRPC(req.Method, req.Params)
	if err != nil {
		writeRPC(w, req.ID, nil, &rpcErr{Code: -32000, Message: err.Error()})
		return
	}
	writeRPC(w, req.ID, result, nil)
}

func writeRPC(w http.ResponseWriter, id any, result any, e *rpcErr) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
		Error:   e,
	})
}

func (s *Server) dispatchRPC(method string, params json.RawMessage) (any, error) {
	switch method {
	case "eth_chainId":
		// numeric chain id as hex quantity
		return fmt.Sprintf("0x%x", s.store.NumericChainID()), nil
	case "net_version":
		return strconv.FormatInt(s.store.NumericChainID(), 10), nil
	case "eth_gasPrice":
		return "0x0", nil // no fee market yet
	case "eth_blockNumber":
		h, err := s.store.GetLatestHeight()
		if err != nil {
			return nil, err
		}
		return fmt.Sprintf("0x%x", h), nil
	case "eth_getBalance":
		var p []string
		if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
			return nil, fmt.Errorf("invalid params")
		}
		acc, err := s.store.GetAccount(p[0])
		if err != nil {
			return nil, err
		}
		// Return hex wei-like quantity; EAST uses 6-decimal subunits as integer.
		return "0x" + big.NewInt(acc.Balance).Text(16), nil
	case "eth_getBlockByNumber":
		var p []any
		if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
			return nil, fmt.Errorf("invalid params")
		}
		height, err := parseBlockTag(fmt.Sprint(p[0]), s)
		if err != nil {
			return nil, err
		}
		if height == 0 {
			return nil, nil
		}
		blk, err := s.store.GetBlock(height)
		if err != nil {
			return nil, nil
		}
		return blockToRPC(blk), nil
	case "eth_getTransactionByHash":
		var p []string
		if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
			return nil, fmt.Errorf("invalid params")
		}
		st, err := s.store.GetTransaction(p[0])
		if err != nil {
			return nil, nil // eth convention: null if not found
		}
		return st, nil
	case "eth_getTransactionCount":
		var p []string
		if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
			return nil, fmt.Errorf("invalid params")
		}
		acc, err := s.store.GetAccount(p[0])
		if err != nil {
			return nil, err
		}
		return fmt.Sprintf("0x%x", acc.Nonce), nil
	case "east_getAccount":
		var p []string
		if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
			return nil, fmt.Errorf("invalid params")
		}
		acc, err := s.store.GetAccount(p[0])
		if err != nil {
			return nil, err
		}
		return acc, nil
	case "east_getProof":
		var p []string
		if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
			return nil, fmt.Errorf("invalid params")
		}
		return s.store.ProveAccount(p[0])
	default:
		return nil, fmt.Errorf("method not found: %s", method)
	}
}

func parseBlockTag(tag string, s *Server) (uint64, error) {
	tag = strings.TrimSpace(strings.ToLower(tag))
	if tag == "latest" || tag == "" {
		return s.store.GetLatestHeight()
	}
	tag = strings.TrimPrefix(tag, "0x")
	v, err := strconv.ParseUint(tag, 16, 64)
	if err != nil {
		// try decimal
		return strconv.ParseUint(tag, 10, 64)
	}
	return v, nil
}

func blockToRPC(h any) map[string]any {
	b, _ := json.Marshal(h)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if height, ok := m["height"].(float64); ok {
		m["number"] = fmt.Sprintf("0x%x", uint64(height))
	}
	m["subunits_per_east"] = tx.SubunitsPerEAST
	return m
}
