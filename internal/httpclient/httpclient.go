package httpclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/antgobar/kvstore/internal/transport"
)

type Client struct {
	ServerAddr string
}

func New(serverAddr string) *Client {
	return &Client{
		ServerAddr: serverAddr,
	}
}

func post[Request any, Response any](c *Client, endpoint string, payload Request) (*Response, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, c.ServerAddr, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusCreated {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("unexpected status" + strconv.Itoa(resp.StatusCode))
	}

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (c *Client) Put(key string, value []byte) error {
	put := transport.KeyValuePayload{
		Key:   key,
		Value: value,
	}
	_, err := post[transport.KeyValuePayload, transport.KeyValuePayload](c, "/put", put)
	return err
}

func (c *Client) Get(key string) (*transport.KeyValuePayload, error) {
	get := transport.KeyPayload{
		Key: key,
	}
	response, err := post[transport.KeyPayload, transport.KeyValuePayload](c, "/get", get)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) Delete(key string) error {
	delete := transport.KeyPayload{
		Key: key,
	}
	_, err := post[transport.KeyPayload, transport.KeyValuePayload](c, "/delete", delete)
	if err != nil {
		return err
	}
	return nil
}
