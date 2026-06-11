package schemaregistry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
)

type Client struct {
	baseURL  string
	username string
	password string
	http     *http.Client

	mu    sync.RWMutex
	cache map[uint32]avro.Schema
}

func NewClient(baseURL, username, password string) *Client {
	return &Client{
		baseURL:  baseURL,
		username: username,
		password: password,
		http:     &http.Client{Timeout: 10 * time.Second},
		cache:    make(map[uint32]avro.Schema),
	}
}

// GetByID returns a parsed Avro schema for the given schema ID, fetching it
// from the Schema Registry on cache miss.
func (c *Client) GetByID(id uint32) (avro.Schema, error) {
	c.mu.RLock()
	if s, ok := c.cache[id]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	c.mu.RUnlock()

	url := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("schema registry request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read schema registry response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry %d for id=%d: %s", resp.StatusCode, id, string(body))
	}

	var payload struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode schema registry response: %w", err)
	}

	parsed, err := avro.Parse(payload.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema id=%d: %w", id, err)
	}

	c.mu.Lock()
	c.cache[id] = parsed
	c.mu.Unlock()
	return parsed, nil
}
