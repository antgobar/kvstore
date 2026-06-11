package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/antgobar/kvstore/transport/http/payload"
)

type HttpClient struct {
	ServerAddr string
	client     http.Client
}

func New(serverAddr string, timeout time.Duration) *HttpClient {
	httpClient := http.Client{Timeout: timeout}
	return &HttpClient{
		ServerAddr: serverAddr,
		client:     httpClient,
	}
}

func (h *HttpClient) Close() error {
	return nil
}

func post[Request any, Response any](ctx context.Context, c *HttpClient, endpoint string, payload Request) (*Response, error) {
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
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
		}

		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(responseBody))
	}

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *HttpClient) Set(ctx context.Context, key string, value []byte) error {
	set := payload.KeyValuePayload{
		Key:   key,
		Value: value,
	}
	_, err := post[payload.KeyValuePayload, any](ctx, c, "/set", set)
	return err
}

func (c *HttpClient) Get(ctx context.Context, key string) ([]byte, error) {
	get := payload.KeyPayload{
		Key: key,
	}
	response, err := post[payload.KeyPayload, payload.ValuePayload](ctx, c, "/get", get)
	if err != nil {
		return nil, err
	}
	return response.Value, nil
}

func (c *HttpClient) Delete(ctx context.Context, key string) error {
	delete := payload.KeyPayload{
		Key: key,
	}
	_, err := post[payload.KeyPayload, any](ctx, c, "/delete", delete)
	if err != nil {
		return err
	}
	return nil
}
