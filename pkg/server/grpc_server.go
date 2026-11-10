package server

import (
	"context"
	"errors"

	"github.com/jathurchan/raft/raft"
	"github.com/jathurchan/raft/types"
	"github.com/mathusanm6/raftkv/pkg/cache"
	"github.com/mathusanm6/raftkv/pkg/raftnode"
	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PeerResolver maps a NodeID to its client gRPC address.
type PeerResolver interface {
	ResolveClientAddress(id types.NodeID) string
}

// GRPCServer implements raftkvv1.RaftKVServer.
type GRPCServer struct {
	raftkvv1.UnimplementedRaftKVServer

	node     raft.Raft
	coalesce *raftnode.ReadIndexCoalescer
	proposer *raftnode.ProposalManager
	peers    PeerResolver
}

// NewGRPCServer creates a new GRPCServer instance.
func NewGRPCServer(
	node raft.Raft,
	coalesce *raftnode.ReadIndexCoalescer,
	proposer *raftnode.ProposalManager,
	peers PeerResolver,
) *GRPCServer {
	return &GRPCServer{
		node:     node,
		coalesce: coalesce,
		proposer: proposer,
		peers:    peers,
	}
}

func (s *GRPCServer) checkLeader() error {
	statusObj := s.node.Status()
	if statusObj.Role != types.RoleLeader {
		leaderID := string(statusObj.LeaderID)
		var leaderAddr string
		if s.peers != nil && leaderID != "" {
			leaderAddr = s.peers.ResolveClientAddress(statusObj.LeaderID)
		}

		st := status.New(codes.FailedPrecondition, "request sent to non-leader replica")
		stWithDetails, err := st.WithDetails(&raftkvv1.LeaderHint{
			LeaderId:      leaderID,
			LeaderAddress: leaderAddr,
		})
		if err == nil {
			return stWithDetails.Err()
		}
		return st.Err()
	}
	return nil
}

// Get serves a linearizable read query.
func (s *GRPCServer) Get(ctx context.Context, req *raftkvv1.GetRequest) (*raftkvv1.GetResponse, error) {
	if len(req.Key) > cache.MaxKeyBytes {
		return nil, status.Error(codes.InvalidArgument, "key exceeds maximum size of 64 bytes")
	}

	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	val, ver, found, err := s.coalesce.Read(ctx, req.Key)
	if err != nil {
		return nil, s.mapError(err)
	}

	return &raftkvv1.GetResponse{
		Found:   found,
		Value:   val,
		Version: ver,
	}, nil
}

// Set serves a client write request.
func (s *GRPCServer) Set(ctx context.Context, req *raftkvv1.SetRequest) (*raftkvv1.SetResponse, error) {
	if len(req.Key) > cache.MaxKeyBytes {
		return nil, status.Error(codes.InvalidArgument, "key exceeds maximum size of 64 bytes")
	}
	if len(req.Value) > cache.MaxValueBytes {
		return nil, status.Error(codes.InvalidArgument, "value exceeds maximum size of 512 bytes")
	}

	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	err := s.proposer.ProposeSet(ctx, req.Key, req.Value, req.Version)
	if err != nil {
		return nil, s.mapError(err)
	}

	return &raftkvv1.SetResponse{}, nil
}

// Delete serves a client delete/invalidation request.
func (s *GRPCServer) Delete(ctx context.Context, req *raftkvv1.DeleteRequest) (*raftkvv1.DeleteResponse, error) {
	if len(req.Key) > cache.MaxKeyBytes {
		return nil, status.Error(codes.InvalidArgument, "key exceeds maximum size of 64 bytes")
	}

	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	err := s.proposer.ProposeDelete(ctx, req.Key, req.Version)
	if err != nil {
		return nil, s.mapError(err)
	}

	return &raftkvv1.DeleteResponse{}, nil
}

func (s *GRPCServer) mapError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, raftnode.ErrResourceExhausted) {
		return status.Error(codes.ResourceExhausted, "cache memory hard limit reached")
	}
	if errors.Is(err, raftnode.ErrNotLeader) {
		return s.checkLeader()
	}
	if errors.Is(err, raftnode.ErrUnavailable) || errors.Is(err, raft.ErrUnavailable) {
		return status.Error(codes.Unavailable, "leader election in progress or term not ready")
	}
	return status.Error(codes.Internal, err.Error())
}
