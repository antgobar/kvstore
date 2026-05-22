package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/antgobar/kvstore/internal/genproto"
	"google.golang.org/grpc"
)

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

// grpcAdapter translates between the proto-generated interface and GrpcServer.
// This keeps proto concerns out of GrpcServer itself.
type grpcAdapter struct {
	pb.UnimplementedKvStoreServer
	srv *GrpcServer
}

func (a *grpcAdapter) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	err := a.srv.Put(ctx, req.Key, req.Value)
	return &pb.PutResponse{}, err
}

func (a *grpcAdapter) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := a.srv.Get(ctx, req.Key)
	return &pb.GetResponse{Value: value}, err
}

func (a *grpcAdapter) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := a.srv.Delete(ctx, req.Key)
	return &pb.DeleteResponse{}, err
}
