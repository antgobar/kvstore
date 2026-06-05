package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/antgobar/kvstore/internal/genproto"
	custom_errors "github.com/antgobar/kvstore/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Storer interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type GrpcServer struct {
	Store   Storer
	Addr    string
	Timeout time.Duration
	server  *grpc.Server
}

func NewGrpcServer(addr string, store Storer, timeout time.Duration) *GrpcServer {
	return &GrpcServer{
		Store:   store,
		Addr:    addr,
		Timeout: timeout,
		server:  grpc.NewServer(),
	}
}

func (s *GrpcServer) Put(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	return s.Store.Put(ctx, key, value)
}

func (s *GrpcServer) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	return s.Store.Get(ctx, key)
}

func (s *GrpcServer) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	return s.Store.Delete(ctx, key)
}

func (s *GrpcServer) Run() {
	lis, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", s.Addr, err)
	}

	pb.RegisterKvStoreServer(s.server, &grpcAdapter{srv: s})
	fmt.Println("Running kvstore GRPC server on", s.Addr)
	if err := s.server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *GrpcServer) Stop() {
	s.server.Stop()
	log.Println("GRPC server stopped")
}

// toGrpcError maps domain errors to gRPC status errors.
func toGrpcError(err error) error {
	if errors.Is(err, custom_errors.ErrKeyNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// grpcAdapter translates between the proto-generated interface and GrpcServer.
// This keeps proto concerns out of GrpcServer itself.
type grpcAdapter struct {
	pb.UnimplementedKvStoreServer
	srv *GrpcServer
}

func (a *grpcAdapter) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := a.srv.Put(ctx, req.Key, req.Value); err != nil {
		return &pb.PutResponse{}, toGrpcError(err)
	}
	return &pb.PutResponse{}, nil
}

func (a *grpcAdapter) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := a.srv.Get(ctx, req.Key)
	if err != nil {
		return &pb.GetResponse{}, toGrpcError(err)
	}
	return &pb.GetResponse{Value: value}, nil
}

func (a *grpcAdapter) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := a.srv.Delete(ctx, req.Key); err != nil {
		return &pb.DeleteResponse{}, toGrpcError(err)
	}
	return &pb.DeleteResponse{}, nil
}
