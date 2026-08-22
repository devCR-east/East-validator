package p2p

import (
	"fmt"
	"net"
	"strings"
)

// NormalizePeerAddr accepts either:
//
//	libp2p multiaddr:  /dns4/host/tcp/4001/p2p/12D3KooW...
//	Cosmos-style:      12D3KooW...@host:26656
//
// and returns a libp2p multiaddr string usable by go-libp2p.
// Format compatibility for EAST operators only — wire protocol remains libp2p.
func NormalizePeerAddr(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty peer addr")
	}
	if strings.HasPrefix(raw, "/") {
		return raw, nil
	}
	at := strings.LastIndex(raw, "@")
	if at <= 0 || at == len(raw)-1 {
		return "", fmt.Errorf("expected multiaddr or id@host:port, got %q", raw)
	}
	id := strings.TrimSpace(raw[:at])
	hostPort := strings.TrimSpace(raw[at+1:])
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		host = hostPort
		port = "26656"
	}
	if id == "" || host == "" {
		return "", fmt.Errorf("invalid id@host:port %q", raw)
	}
	if ip := net.ParseIP(host); ip != nil {
		return fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s", host, port, id), nil
	}
	return fmt.Sprintf("/dns4/%s/tcp/%s/p2p/%s", host, port, id), nil
}

// NormalizePeerAddrList splits on comma and normalizes each entry.
func NormalizePeerAddrList(raw string) []string {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := NormalizePeerAddr(p)
		if err != nil {
			out = append(out, p)
			continue
		}
		out = append(out, n)
	}
	return out
}
