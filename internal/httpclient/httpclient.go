package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Client struct {
	ServerAddr string
}

func New(serverAddr string) *Client {
	return &Client{
		ServerAddr: serverAddr,
	}
}

type putKey struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}


type (c *Client) Post[Request any, Response any](endpoint string, payload Request) (*Response, error)  {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, c.ServerAddr, bytes.NewReader(data))
	if err = nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err = nil {
		return err
	}
	defer resp.Body.Close()
	req.Header.Set("Content-Type", "application/json")

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &result, nil
}


func (c *Client) Put(key string, value []byte) (any, error) {
	putKeyData := new(putKey)
	putKeyData.Key = key
	putKeyData.Value = string(value)

	resp, err := c.post("/put", putKeyData)
}
