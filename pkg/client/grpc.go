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
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	client := pb.NewKvStoreClient(conn)

	return &GrpcClient{client, conn}
}

func (g *GrpcClient) Put(ctx context.Context, key string, value []byte) error {
	_, err := g.client.Put(ctx, &pb.PutRequest{
		Key:   key,
		Value: value,
	})
	return err
}

func (g *GrpcClient) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := g.client.Get(ctx, &pb.GetRequest{
		Key: key,
	})
	return res.Value, err
}

func (g *GrpcClient) Delete(ctx context.Context, key string) error {
	_, err := g.client.Delete(ctx, &pb.DeleteRequest{
		Key: key,
	})
	return err
}
