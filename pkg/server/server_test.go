package server

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jathurchan/raft/raft"
	"github.com/jathurchan/raft/types"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/eviction"
	"github.com/mathusanm6/raftkv/pkg/raftnode"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type mockRaft struct {
	raft.RPCHandler

	isLeader    bool
	term        types.Term
	leaderID    types.NodeID
	commitIndex types.Index
	lastApplied types.Index
	applier     *raftnode.Applier
	proposeErr  error
}

func (m *mockRaft) SetNetworkManager(nm raft.NetworkManager) {}
func (m *mockRaft) Start() error                             { return nil }
func (m *mockRaft) Stop(ctx context.Context) error           { return nil }
func (m *mockRaft) Tick(ctx context.Context)                 {}

func (m *mockRaft) Propose(ctx context.Context, cmd []byte) (types.Index, types.Term, bool, error) {
	if m.proposeErr != nil {
		return 0, 0, m.isLeader, m.proposeErr
	}
	if !m.isLeader {
		return 0, 0, false, raftnode.ErrNotLeader
	}
	m.commitIndex++
	m.lastApplied = m.commitIndex
	if m.applier != nil {
		_, _ = m.applier.Apply(ctx, m.commitIndex, cmd)
	}
	return m.commitIndex, m.term, true, nil
}

func (m *mockRaft) ReadIndex(ctx context.Context) (types.Index, error) {
	if !m.isLeader {
		return 0, raft.ErrNotLeader
	}
	return m.commitIndex, nil
}

func (m *mockRaft) Status() types.RaftStatus {
	role := types.RoleFollower
	if m.isLeader {
		role = types.RoleLeader
	}
	return types.RaftStatus{
		ID:          "node-0",
		Role:        role,
		Term:        m.term,
		LeaderID:    m.leaderID,
		CommitIndex: m.commitIndex,
		LastApplied: m.lastApplied,
	}
}

func (m *mockRaft) GetState() (types.Term, bool)             { return m.term, m.isLeader }
func (m *mockRaft) GetLeaderID() types.NodeID                { return m.leaderID }
func (m *mockRaft) GetCommitIndex() types.Index              { return m.commitIndex }
func (m *mockRaft) ApplyChannel() <-chan types.ApplyMsg      { return nil }
func (m *mockRaft) LeaderChangeChannel() <-chan types.NodeID { return nil }

type mockResolver struct{}

func (mockResolver) ResolveClientAddress(id types.NodeID) string {
	return "127.0.0.1:9000"
}

func TestGRPCServerOperations(t *testing.T) {
	store := cache.NewStore()
	lru := eviction.NewLRU()
	applier := raftnode.NewApplier(store, lru)

	mockNode := &mockRaft{
		isLeader:    true,
		term:        1,
		leaderID:    "node-0",
		commitIndex: 0,
		lastApplied: 0,
		applier:     applier,
	}

	coalescer := raftnode.NewReadIndexCoalescer(mockNode, store, lru)
	defer coalescer.Stop()

	pm := raftnode.NewProposalManager(mockNode, store, applier, lru)
	grpcSvc := NewGRPCServer(mockNode, coalescer, pm, mockResolver{})

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	raftkvv1.RegisterRaftKVServer(grpcServer, grpcSvc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := raftkvv1.NewRaftKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. SET
	_, err = client.Set(ctx, &raftkvv1.SetRequest{
		Key:     "item-1",
		Value:   "val-1",
		Version: 1,
	})
	require.NoError(t, err)

	// 2. GET
	getResp, err := client.Get(ctx, &raftkvv1.GetRequest{Key: "item-1"})
	require.NoError(t, err)
	require.True(t, getResp.Found)
	require.Equal(t, "val-1", getResp.Value)
	require.Equal(t, int64(1), getResp.Version)

	// 3. DELETE
	_, err = client.Delete(ctx, &raftkvv1.DeleteRequest{
		Key:     "item-1",
		Version: 1,
	})
	require.NoError(t, err)

	// 4. GET after delete
	getResp, err = client.Get(ctx, &raftkvv1.GetRequest{Key: "item-1"})
	require.NoError(t, err)
	require.False(t, getResp.Found)
}

func TestGRPCServerFollowerRedirection(t *testing.T) {
	store := cache.NewStore()
	lru := eviction.NewLRU()
	applier := raftnode.NewApplier(store, lru)

	// Follower node pointing to leader "node-1"
	mockNode := &mockRaft{
		isLeader: false,
		term:     1,
		leaderID: "node-1",
		applier:  applier,
	}

	coalescer := raftnode.NewReadIndexCoalescer(mockNode, store, lru)
	defer coalescer.Stop()

	pm := raftnode.NewProposalManager(mockNode, store, applier, lru)
	grpcSvc := NewGRPCServer(mockNode, coalescer, pm, mockResolver{})

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	raftkvv1.RegisterRaftKVServer(grpcServer, grpcSvc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := raftkvv1.NewRaftKVClient(conn)
	ctx := context.Background()

	_, err = client.Get(ctx, &raftkvv1.GetRequest{Key: "item-1"})
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())

	// Verify LeaderHint in details
	details := st.Details()
	require.Len(t, details, 1)
	hint, ok := details[0].(*raftkvv1.LeaderHint)
	require.True(t, ok)
	require.Equal(t, "node-1", hint.LeaderId)
	require.Equal(t, "127.0.0.1:9000", hint.LeaderAddress)
}

func TestHTTPServerEndpoints(t *testing.T) {
	store := cache.NewStore()
	lru := eviction.NewLRU()
	mockNode := &mockRaft{
		isLeader: true,
		term:     2,
		leaderID: "node-0",
	}

	httpSvc := NewHTTPServer(mockNode, store, lru, mockResolver{})
	handler := httpSvc.Handler()

	// 1. /livez
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// 2. /readyz
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// 3. /status
	req = httptest.NewRequest(http.MethodGet, "/status", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var statusObj StatusResponse
	err := json.Unmarshal(rec.Body.Bytes(), &statusObj)
	require.NoError(t, err)
	require.Equal(t, "leader", statusObj.Role)
	require.Equal(t, uint64(2), statusObj.Term)
	require.Equal(t, "node-0", statusObj.LeaderID)

	// 4. /debug/reset-policy
	req = httptest.NewRequest(http.MethodPost, "/debug/reset-policy", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}
