package shardedkv

import (
	"context"
	"errors"

	raftkvv1 "github.com/mathusanm6/raftkv/proto/raftkv/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements raftkvv1.RaftKVServer for a standalone shard.
type GRPCServer struct {
	raftkvv1.UnimplementedRaftKVServer
	shard *Shard
}

// NewGRPCServer creates a new GRPCServer instance for a shard.
func NewGRPCServer(shard *Shard) *GRPCServer {
	return &GRPCServer{shard: shard}
}

// Get handles cache read requests.
func (s *GRPCServer) Get(ctx context.Context, req *raftkvv1.GetRequest) (*raftkvv1.GetResponse, error) {
	val, ver, found, err := s.shard.Get(ctx, req.Key)
	if err != nil {
		return nil, s.mapError(err)
	}

	return &raftkvv1.GetResponse{
		Found:   found,
		Value:   val,
		Version: ver,
	}, nil
}

// Set handles cache insert/update requests.
func (s *GRPCServer) Set(ctx context.Context, req *raftkvv1.SetRequest) (*raftkvv1.SetResponse, error) {
	err := s.shard.Set(ctx, req.Key, req.Value, req.Version)
	if err != nil {
		return nil, s.mapError(err)
	}

	return &raftkvv1.SetResponse{}, nil
}

// Delete handles cache invalidation requests.
func (s *GRPCServer) Delete(ctx context.Context, req *raftkvv1.DeleteRequest) (*raftkvv1.DeleteResponse, error) {
	err := s.shard.Delete(ctx, req.Key, req.Version)
	if err != nil {
		return nil, s.mapError(err)
	}

	return &raftkvv1.DeleteResponse{}, nil
}

func (s *GRPCServer) mapError(err error) error {
	if errors.Is(err, ErrResourceExhausted) {
		return status.Error(codes.ResourceExhausted, "shard memory full, cannot evict")
	}
	if errors.Is(err, ErrKeyTooLarge) || errors.Is(err, ErrValueTooLarge) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	return status.Error(codes.Internal, err.Error())
}
