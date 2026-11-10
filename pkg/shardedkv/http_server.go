package shardedkv

import (
	"encoding/json"
	"net/http"
)

// HTTPServer provides management and health check endpoints for a shard.
type HTTPServer struct {
	shardID string
	shard   *Shard
}

// NewHTTPServer creates an HTTPServer for a shard.
func NewHTTPServer(shardID string, shard *Shard) *HTTPServer {
	return &HTTPServer{
		shardID: shardID,
		shard:   shard,
	}
}

// Handler returns the HTTP mux for the shard.
func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", s.handleLivez)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

func (s *HTTPServer) handleLivez(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *HTTPServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

type StatusResponse struct {
	ShardID             string `json:"shard_id"`
	AccountedEntryBytes int64  `json:"accounted_entry_bytes"`
	TotalAccountedBytes int64  `json:"total_accounted_bytes"`
	EntryCount          int    `json:"entry_count"`
	TombstoneCount      int    `json:"tombstone_count"`
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	store := s.shard.Store()
	resp := StatusResponse{
		ShardID:             s.shardID,
		AccountedEntryBytes: store.MemoryTracker().EntryBytes(),
		TotalAccountedBytes: store.MemoryTracker().TotalAccountedBytes(),
		EntryCount:          store.Len(),
		TombstoneCount:      store.Tombstones().Count(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *HTTPServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("# HELP shardedkv_status shardedkv operational status\n# TYPE shardedkv_status gauge\n"))
}
