// Package client is a Go client for a quorumkv cluster. It finds the leader,
// follows redirects, and retries with the same (client id, sequence number)
// so writes are applied exactly once even across timeouts and failovers.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/eshaan2418/quorumkv/server"
)

// ErrUnknown is returned when the deadline passes before the cluster
// confirmed the operation; a write may or may not have been applied.
var ErrUnknown = errors.New("client: outcome unknown (deadline exceeded)")

type Client struct {
	addrs   []string
	http    *http.Client
	attempt time.Duration

	mu     sync.Mutex
	spread bool
	id     uint64
	seq    uint64
	leader string
	rng    *mrand.Rand
}

// New creates a client for the given server addresses (host:port).
// attemptTimeout bounds each individual HTTP request.
func New(addrs []string, attemptTimeout time.Duration) *Client {
	var b [8]byte
	rand.Read(b[:])
	return &Client{
		addrs:   addrs,
		http:    &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}},
		attempt: attemptTimeout,
		id:      binary.LittleEndian.Uint64(b[:]) | 1,
		rng:     mrand.New(mrand.NewSource(int64(binary.LittleEndian.Uint64(b[:])))),
	}
}

// SpreadReads makes each Get start at a random server instead of the
// known leader, like a load balancer doing follower reads. With linearizable
// read modes the server redirects to the leader; with unsafe local reads it
// exposes stale data.
func (c *Client) SpreadReads(on bool) {
	c.mu.Lock()
	c.spread = on
	c.mu.Unlock()
}

func (c *Client) Get(ctx context.Context, key string) (value string, found bool, err error) {
	r, err := c.Do(ctx, server.KVRequest{Op: "get", Key: key})
	return r.Value, r.Found, err
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.Do(ctx, server.KVRequest{Op: "put", Key: key, Value: value})
	return err
}

func (c *Client) Append(ctx context.Context, key, value string) error {
	_, err := c.Do(ctx, server.KVRequest{Op: "append", Key: key, Value: value})
	return err
}

func (c *Client) Delete(ctx context.Context, key string) (existed bool, err error) {
	r, err := c.Do(ctx, server.KVRequest{Op: "delete", Key: key})
	return r.Found, err
}

// Do sends a request, retrying until ctx expires. A Client must not be used
// for concurrent writes (one outstanding write per session).
func (c *Client) Do(ctx context.Context, req server.KVRequest) (server.KVResponse, error) {
	c.mu.Lock()
	if req.Op != "get" {
		c.seq++
		req.ClientID, req.Seq = c.id, c.seq
	}
	target := c.leader
	if req.Op == "get" && c.spread {
		target = c.addrs[c.rng.Intn(len(c.addrs))]
	}
	c.mu.Unlock()
	body, _ := json.Marshal(req)
	backoff := 5 * time.Millisecond
	for {
		if target == "" {
			c.mu.Lock()
			target = c.addrs[c.rng.Intn(len(c.addrs))]
			c.mu.Unlock()
		}
		resp, err := c.send(ctx, target, body)
		if err == nil && resp.OK {
			c.mu.Lock()
			c.leader = target
			c.mu.Unlock()
			return resp, nil
		}
		next := ""
		if err == nil && resp.Error == "not_leader" && resp.Leader != "" && resp.Leader != target {
			next = resp.Leader
		} else if err == nil && resp.Error == "bad_request" {
			return resp, fmt.Errorf("client: bad request")
		}
		c.mu.Lock()
		c.leader = next
		c.mu.Unlock()
		target = next
		if next == "" {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return server.KVResponse{}, ErrUnknown
			}
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
		}
		if ctx.Err() != nil {
			return server.KVResponse{}, ErrUnknown
		}
	}
}

func (c *Client) send(ctx context.Context, addr string, body []byte) (server.KVResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.attempt)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/kv", bytes.NewReader(body))
	if err != nil {
		return server.KVResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return server.KVResponse{}, err
	}
	defer resp.Body.Close()
	var out server.KVResponse
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}
