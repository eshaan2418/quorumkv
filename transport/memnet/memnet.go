// Package memnet is an in-process network for testing Raft under faults:
// directional partitions, message loss, and random delay (which also causes
// reordering). It is deterministic in structure but uses real time.
package memnet

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eshaan2418/quorumkv/raft"
)

var ErrUnreachable = errors.New("memnet: unreachable")

type Network struct {
	mu       sync.Mutex
	handlers map[int]raft.Handler
	cut      map[[2]int]bool // from -> to is blocked
	drop     float64
	minDelay time.Duration
	maxDelay time.Duration
	rng      *rand.Rand

	RPCs atomic.Int64
}

func New(seed int64) *Network {
	return &Network{handlers: map[int]raft.Handler{}, cut: map[[2]int]bool{}, rng: rand.New(rand.NewSource(seed))}
}

// Register connects a node; Unregister models a crash (RPCs to it fail).
func (n *Network) Register(id int, h raft.Handler) {
	n.mu.Lock()
	n.handlers[id] = h
	n.mu.Unlock()
}

func (n *Network) Unregister(id int) {
	n.mu.Lock()
	delete(n.handlers, id)
	n.mu.Unlock()
}

// Partition splits nodes into groups that cannot reach each other. Nodes not
// listed keep full connectivity with everyone.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = map[[2]int]bool{}
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					n.cut[[2]int{a, b}] = true
				}
			}
		}
	}
}

// Cut blocks messages from a to b only (asymmetric partition).
func (n *Network) Cut(a, b int) {
	n.mu.Lock()
	n.cut[[2]int{a, b}] = true
	n.mu.Unlock()
}

func (n *Network) Heal() {
	n.mu.Lock()
	n.cut = map[[2]int]bool{}
	n.mu.Unlock()
}

// SetUnreliable drops each request and each reply with probability drop and
// delays deliveries uniformly in [minDelay, maxDelay].
func (n *Network) SetUnreliable(drop float64, minDelay, maxDelay time.Duration) {
	n.mu.Lock()
	n.drop, n.minDelay, n.maxDelay = drop, minDelay, maxDelay
	n.mu.Unlock()
}

func (n *Network) plan(from, to int) (h raft.Handler, delay time.Duration, dropReq, dropResp bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h = n.handlers[to]
	if n.cut[[2]int{from, to}] || n.cut[[2]int{to, from}] {
		h = nil
	}
	if n.maxDelay > 0 {
		delay = n.minDelay + time.Duration(n.rng.Int63n(int64(n.maxDelay-n.minDelay)+1))
	}
	dropReq = n.rng.Float64() < n.drop
	dropResp = n.rng.Float64() < n.drop
	return
}

func call[T any](ctx context.Context, n *Network, from, to int, fn func(raft.Handler) T) (T, error) {
	var zero T
	n.RPCs.Add(1)
	h, delay, dropReq, dropResp := n.plan(from, to)
	if h == nil || dropReq {
		// Lost messages still cost time, like a real timeout.
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
		return zero, ErrUnreachable
	}
	done := make(chan T, 1)
	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		// Re-check reachability at delivery time.
		if h2, _, _, _ := n.plan(from, to); h2 == nil {
			return
		}
		done <- fn(h)
	}()
	select {
	case r := <-done:
		if dropResp {
			return zero, ErrUnreachable
		}
		return r, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

type transport struct {
	net  *Network
	from int
}

// Transport returns the raft.Transport used by node `from`.
func (n *Network) Transport(from int) raft.Transport { return &transport{net: n, from: from} }

func (t *transport) RequestVote(ctx context.Context, to int, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return call(ctx, t.net, t.from, to, func(h raft.Handler) *raft.RequestVoteReply { return h.HandleRequestVote(a) })
}

func (t *transport) AppendEntries(ctx context.Context, to int, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return call(ctx, t.net, t.from, to, func(h raft.Handler) *raft.AppendEntriesReply { return h.HandleAppendEntries(a) })
}

func (t *transport) InstallSnapshot(ctx context.Context, to int, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return call(ctx, t.net, t.from, to, func(h raft.Handler) *raft.InstallSnapshotReply { return h.HandleInstallSnapshot(a) })
}
