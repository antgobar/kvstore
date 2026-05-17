package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/antgobar/kvstore/internal/transport"
)

type Client struct {
	ServerAddr string
	client     http.Client
}

func New(serverAddr string, timeout time.Duration) *Client {
	httpClient := http.Client{Timeout: timeout}
	return &Client{
		ServerAddr: serverAddr,
		client:     httpClient,
	}
}

func post[Request any, Response any](ctx context.Context, c *Client, endpoint string, payload Request) (*Response, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ServerAddr+endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusCreated {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	put := transport.KeyValuePayload{
		Key:   key,
		Value: value,
	}
	_, err := post[transport.KeyValuePayload, any](ctx, c, "/put", put)
	return err
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	get := transport.KeyPayload{
		Key: key,
	}
	response, err := post[transport.KeyPayload, transport.ValuePayload](ctx, c, "/get", get)
	if err != nil {
		return nil, err
	}
	return response.Value, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	delete := transport.KeyPayload{
		Key: key,
	}
	_, err := post[transport.KeyPayload, any](ctx, c, "/delete", delete)
	if err != nil {
		return err
	}
	return nil
}
