package raftnode

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jathurchan/raft/raft"
	"github.com/mathusanm6/raftkv/pkg/cache"
)

// ReadRequest encapsulates a client read query awaiting linearizable verification.
type ReadRequest struct {
	Key     string
	Ctx     context.Context
	ReplyCh chan ReadResponse
}

// ReadResponse holds the result of a linearizable read.
type ReadResponse struct {
	Value   string
	Version int64
	Found   bool
	Err     error
}

// AccessRecorder receives cache hits served by the leader. It is either the
// eviction policy itself (v1) or an AccessReplicator (v2, design doc §7.6).
type AccessRecorder interface {
	RecordAccess(key string)
}

// ReadIndexCoalescer groups concurrent reads into single ReadIndex rounds.
// If a round is in flight, new arrivals join the next round.
type ReadIndexCoalescer struct {
	mu sync.Mutex

	node     raft.Raft
	store    *cache.Store
	eviction AccessRecorder

	activeRound   []*ReadRequest
	nextRound     []*ReadRequest
	roundInFlight bool

	stopCh chan struct{}
}

// NewReadIndexCoalescer creates a new ReadIndexCoalescer.
func NewReadIndexCoalescer(node raft.Raft, store *cache.Store, policy AccessRecorder) *ReadIndexCoalescer {
	return &ReadIndexCoalescer{
		node:     node,
		store:    store,
		eviction: policy,
		stopCh:   make(chan struct{}),
	}
}

// Read processes a linearizable read request.
func (c *ReadIndexCoalescer) Read(ctx context.Context, key string) (string, int64, bool, error) {
	replyCh := make(chan ReadResponse, 1)
	req := &ReadRequest{
		Key:     key,
		Ctx:     ctx,
		ReplyCh: replyCh,
	}

	c.mu.Lock()
	// Next-round joining: If a round is currently in flight, join nextRound.
	if c.roundInFlight {
		c.nextRound = append(c.nextRound, req)
		c.mu.Unlock()
	} else {
		// No round in flight: become the active round and launch it
		c.activeRound = append(c.activeRound, req)
		c.roundInFlight = true
		c.mu.Unlock()

		go c.executeRound()
	}

	select {
	case <-ctx.Done():
		return "", 0, false, ctx.Err()
	case <-c.stopCh:
		return "", 0, false, errors.New("read coalescer shut down")
	case resp := <-replyCh:
		return resp.Value, resp.Version, resp.Found, resp.Err
	}
}

// executeRound runs a single ReadIndex quorum round for all requests in activeRound.
func (c *ReadIndexCoalescer) executeRound() {
	for {
		c.mu.Lock()
		requests := c.activeRound
		c.activeRound = nil
		c.mu.Unlock()

		if len(requests) == 0 {
			c.mu.Lock()
			c.roundInFlight = false
			c.mu.Unlock()
			return
		}

		c.processBatch(requests)

		c.mu.Lock()
		if len(c.nextRound) > 0 {
			c.activeRound = c.nextRound
			c.nextRound = nil
			c.mu.Unlock()
			// Continue loop to process the next round
		} else {
			c.roundInFlight = false
			c.mu.Unlock()
			return
		}
	}
}

func (c *ReadIndexCoalescer) processBatch(requests []*ReadRequest) {
	// Create a context bounded by the earliest deadline among valid requests
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	commitIndex, err := c.node.ReadIndex(ctx)
	if err != nil {
		for _, req := range requests {
			req.ReplyCh <- ReadResponse{Err: err}
		}
		return
	}

	// Wait until local state reaches commitIndex: Status().LastApplied >= commitIndex
	targetIndex := uint64(commitIndex)
	for {
		status := c.node.Status()
		if uint64(status.LastApplied) >= targetIndex {
			break
		}

		select {
		case <-ctx.Done():
			for _, req := range requests {
				req.ReplyCh <- ReadResponse{Err: ctx.Err()}
			}
			return
		case <-c.stopCh:
			for _, req := range requests {
				req.ReplyCh <- ReadResponse{Err: errors.New("read coalescer stopped")}
			}
			return
		case <-time.After(1 * time.Millisecond):
		}
	}

	// Serve all requests from local cache store
	for _, req := range requests {
		if req.Ctx.Err() != nil {
			req.ReplyCh <- ReadResponse{Err: req.Ctx.Err()}
			continue
		}

		val, ver, found := c.store.Get(req.Key)
		if found && c.eviction != nil {
			c.eviction.RecordAccess(req.Key)
		}

		req.ReplyCh <- ReadResponse{
			Value:   val,
			Version: ver,
			Found:   found,
			Err:     nil,
		}
	}
}

// AbortAll cancels any queued requests.
func (c *ReadIndexCoalescer) AbortAll(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, req := range c.activeRound {
		req.ReplyCh <- ReadResponse{Err: err}
	}
	c.activeRound = nil

	for _, req := range c.nextRound {
		req.ReplyCh <- ReadResponse{Err: err}
	}
	c.nextRound = nil
}

// Stop shuts down the coalescer.
func (c *ReadIndexCoalescer) Stop() {
	close(c.stopCh)
	c.AbortAll(errors.New("coalescer stopped"))
}
