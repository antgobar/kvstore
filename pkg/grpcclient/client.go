package grpcclient

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/antgobar/kvstore/internal/genproto"
)

type GrpcClient struct {
	client     pb.KvStoreClient
	connection *grpc.ClientConn
	timeout    time.Duration
}

func (s *GrpcClient) Close() error {
	return s.connection.Close()
}

func New(addr string, timeout time.Duration) *GrpcClient {
	conn, err := grpc.NewClient("dns:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	client := pb.NewKvStoreClient(conn)

	return &GrpcClient{client, conn, timeout}
}

func (s *GrpcClient) Put(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.client.Put(ctx, &pb.PutRequest{
		Key:   key,
		Value: value,
	})
	return err
}

func (s *GrpcClient) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.client.Get(ctx, &pb.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

func (s *GrpcClient) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.client.Delete(ctx, &pb.DeleteRequest{
		Key: key,
	})
	return err
}
