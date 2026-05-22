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
	pb.UnimplementedKvStoreServer
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

func (s *GrpcServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	err := s.Store.Put(ctx, req.Key, req.Value)
	return &pb.PutResponse{}, err
}

func (s *GrpcServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	value, err := s.Store.Get(ctx, req.Key)
	return &pb.GetResponse{
		Value: value,
	}, err
}

func (s *GrpcServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	err := s.Store.Delete(ctx, req.Key)
	return &pb.DeleteResponse{}, err
}

func (s *GrpcServer) Run() {
	lis, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", s.Addr, err)
	}

	pb.RegisterKvStoreServer(s.server, s)
	fmt.Println("Running kvstore GRPC server on", s.Addr)
	if err := s.server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

func (s *GrpcServer) Stop() {
	s.server.Stop()
	log.Println("GRPC server stopped")
}
