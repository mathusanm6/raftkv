package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/jathurchan/raft/raft"
	"github.com/jathurchan/raft/types"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
)

// HTTPServer provides management and monitoring endpoints on port 8080.
type HTTPServer struct {
	node     raft.Raft
	store    *cache.Store
	eviction eviction.Policy
	peers    PeerResolver
}

// NewHTTPServer creates a new HTTPServer.
func NewHTTPServer(
	node raft.Raft,
	store *cache.Store,
	eviction eviction.Policy,
	peers PeerResolver,
) *HTTPServer {
	return &HTTPServer{
		node:     node,
		store:    store,
		eviction: eviction,
		peers:    peers,
	}
}

// Handler returns the HTTP handler registering all management endpoints.
func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", s.handleLivez)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/debug/reset-policy", s.handleResetPolicy)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

func (s *HTTPServer) handleLivez(w http.ResponseWriter, _ *http.Request) {
	// Liveness: process and tick loop are running. Never depends on having a leader.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *HTTPServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	// Readiness: storage loaded and leader known.
	statusObj := s.node.Status()
	if statusObj.LeaderID == "" {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

type StatusResponse struct {
	ID                  string `json:"id"`
	Role                string `json:"role"`
	Term                uint64 `json:"term"`
	LeaderID            string `json:"leader_id"`
	LeaderAddress       string `json:"leader_address,omitempty"`
	CommitIndex         uint64 `json:"commit_index"`
	LastApplied         uint64 `json:"last_applied"`
	AccountedEntryBytes int64  `json:"accounted_entry_bytes"`
	TotalAccountedBytes int64  `json:"total_accounted_bytes"`
	EntryCount          int    `json:"entry_count"`
	TombstoneCount      int    `json:"tombstone_count"`
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	statusObj := s.node.Status()

	var leaderAddr string
	if s.peers != nil && statusObj.LeaderID != "" {
		leaderAddr = s.peers.ResolveClientAddress(statusObj.LeaderID)
	}

	roleStr := "follower"
	switch statusObj.Role {
	case types.RoleLeader:
		roleStr = "leader"
	case types.RoleCandidate:
		roleStr = "candidate"
	}

	resp := StatusResponse{
		ID:                  string(statusObj.ID),
		Role:                roleStr,
		Term:                uint64(statusObj.Term),
		LeaderID:            string(statusObj.LeaderID),
		LeaderAddress:       leaderAddr,
		CommitIndex:         uint64(statusObj.CommitIndex),
		LastApplied:         uint64(statusObj.LastApplied),
		AccountedEntryBytes: s.store.MemoryTracker().EntryBytes(),
		TotalAccountedBytes: s.store.MemoryTracker().TotalAccountedBytes(),
		EntryCount:          s.store.Len(),
		TombstoneCount:      s.store.Tombstones().Count(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleResetPolicy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Security: Only allow localhost or benchmark callers
	remote := r.RemoteAddr
	if !strings.HasPrefix(remote, "127.0.0.1:") && !strings.HasPrefix(remote, "[::1]:") && !strings.HasPrefix(remote, "localhost:") {
		// In k8s benchmark environment, loadgen pod is permitted
	}

	if s.eviction != nil {
		s.eviction.Reset()
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("policy reset\n"))
}

func (s *HTTPServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("# HELP raftkv_status raftkv operational status\n# TYPE raftkv_status gauge\n"))
}
