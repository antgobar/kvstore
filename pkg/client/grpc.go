package client

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/antgobar/kvstore/internal/genproto"
)

type GrpcClient struct {
	client     pb.KvStoreClient
	connection *grpc.ClientConn
}

func NewGrpcClient(addr string) *GrpcClient {
	conn, err := grpc.NewClient("dns:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	client := pb.NewKvStoreClient(conn)

	return &GrpcClient{client, conn}
}

func (s *GrpcClient) Put(ctx context.Context, key string, value []byte) error {
	defer s.connection.Close()
	_, err := s.client.Put(ctx, &pb.PutRequest{
		Key:   key,
		Value: value,
	})
	return err
}

func (s *GrpcClient) Get(ctx context.Context, key string) ([]byte, error) {
	defer s.connection.Close()
	res, err := s.client.Get(ctx, &pb.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return res.Value, nil
}

func (s *GrpcClient) Delete(ctx context.Context, key string) error {
	defer s.connection.Close()
	_, err := s.client.Delete(ctx, &pb.DeleteRequest{
		Key: key,
	})
	return err
}
