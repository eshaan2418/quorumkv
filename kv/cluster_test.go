package kv_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eshaan2418/quorumkv/kv"
	"github.com/eshaan2418/quorumkv/lin"
	"github.com/eshaan2418/quorumkv/raft"
	"github.com/eshaan2418/quorumkv/transport/memnet"
)

const electionTimeout = 100 * time.Millisecond

type cluster struct {
	t      *testing.T
	n      int
	net    *memnet.Network
	opts   kv.Options
	mu     sync.Mutex
	stores map[int]*kv.Store
	nodes  map[int]*raft.Node
	chans  map[int]chan raft.ApplyMsg
	disk   map[int]*raft.MemStorage
}

func newCluster(t *testing.T, n int, seed int64, opts kv.Options) *cluster {
	c := &cluster{t: t, n: n, net: memnet.New(seed), opts: opts, stores: map[int]*kv.Store{},
		nodes: map[int]*raft.Node{}, chans: map[int]chan raft.ApplyMsg{}, disk: map[int]*raft.MemStorage{}}
	for id := 1; id <= n; id++ {
		c.disk[id] = raft.NewMemStorage()
		c.start(id)
	}
	t.Cleanup(func() {
		for id := 1; id <= n; id++ {
			c.crash(id)
		}
	})
	return c
}

func (c *cluster) start(id int) {
	peers := make([]int, c.n)
	for i := range peers {
		peers[i] = i + 1
	}
	ch := make(chan raft.ApplyMsg, 256)
	st := kv.New(c.opts)
	node, err := raft.New(raft.Config{ID: id, Peers: peers, Transport: c.net.Transport(id), Storage: c.disk[id],
		Apply: ch, ElectionTimeout: electionTimeout, HeartbeatInterval: 20 * time.Millisecond})
	if err != nil {
		c.t.Fatal(err)
	}
	st.Run(node, ch)
	c.mu.Lock()
	c.stores[id], c.nodes[id], c.chans[id] = st, node, ch
	c.mu.Unlock()
	c.net.Register(id, node)
}

func (c *cluster) crash(id int) {
	c.mu.Lock()
	node, ch := c.nodes[id], c.chans[id]
	delete(c.nodes, id)
	delete(c.stores, id)
	c.mu.Unlock()
	if node == nil {
		return
	}
	c.net.Unregister(id)
	node.Stop()
	close(ch)
}

func (c *cluster) store(id int) *kv.Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[id]
}

func (c *cluster) leader() int {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for id, n := range c.nodes {
			if n.Status().Role == raft.Leader {
				c.mu.Unlock()
				return id
			}
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0
}

// ---------------------------------------------------------------------------
// A retrying client that records a history for the checker.

type recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []lin.Op
}

func (r *recorder) now() int64 { return int64(time.Since(r.start)) }

func (r *recorder) add(op lin.Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

var clientIDs atomic.Uint64

type client struct {
	c      *cluster
	id     uint64
	seq    uint64
	rng    *rand.Rand
	target int
	rec    *recorder
	num    int
}

func newClient(c *cluster, rec *recorder, seed int64, num int) *client {
	return &client{c: c, id: clientIDs.Add(1), rng: rand.New(rand.NewSource(seed)), target: 1, rec: rec, num: num}
}

// do retries a request across nodes until it succeeds or the deadline
// passes. ok=false means the outcome is unknown.
func (cl *client) do(req kv.Request, overall time.Duration) (kv.Response, bool) {
	if req.Kind != kv.OpGet {
		cl.seq++
		req.ClientID, req.Seq = cl.id, cl.seq
	}
	deadline := time.Now().Add(overall)
	for time.Now().Before(deadline) {
		st := cl.c.store(cl.target)
		if st != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			resp, err := st.Do(ctx, req)
			cancel()
			if err == nil {
				return resp, true
			}
			if !errors.Is(err, kv.ErrNotLeader) && !errors.Is(err, kv.ErrUnknown) &&
				!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, raft.ErrStopped) {
				cl.c.t.Errorf("unexpected error: %v", err)
			}
		}
		cl.target = 1 + cl.rng.Intn(cl.c.n)
		time.Sleep(time.Duration(cl.rng.Intn(10)) * time.Millisecond)
	}
	return kv.Response{}, false
}

func (cl *client) run(stop <-chan struct{}, keys int) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprint("k", cl.rng.Intn(keys))
		var req kv.Request
		op := lin.Op{Client: cl.num, Key: key}
		switch cl.rng.Intn(3) {
		case 0:
			req, op.Kind = kv.Request{Kind: kv.OpGet, Key: key}, lin.Get
		case 1:
			v := fmt.Sprintf("%d.%d ", cl.num, cl.seq+1)
			req, op.Kind, op.Value = kv.Request{Kind: kv.OpPut, Key: key, Value: v}, lin.Put, v
		default:
			v := fmt.Sprintf("%d.%d ", cl.num, cl.seq+1)
			req, op.Kind, op.Value = kv.Request{Kind: kv.OpAppend, Key: key, Value: v}, lin.Append, v
		}
		op.Call = cl.rec.now()
		resp, ok := cl.do(req, 2*time.Second)
		op.Return = cl.rec.now()
		switch {
		case ok:
			op.Output, op.Found = resp.Value, resp.Found
			cl.rec.add(op)
		case op.Kind != lin.Get:
			op.Return = lin.Pending
			cl.rec.add(op)
		}
	}
}

// ---------------------------------------------------------------------------

func runWorkload(t *testing.T, seed int64, opts kv.Options, d time.Duration) lin.Result {
	c := newCluster(t, 5, seed, opts)
	c.net.SetUnreliable(0.05, 0, 10*time.Millisecond)
	c.leader()
	rec := &recorder{start: time.Now()}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		cl := newClient(c, rec, seed*31+int64(i), i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl.run(stop, 4)
		}()
	}
	rng := rand.New(rand.NewSource(seed))
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		switch rng.Intn(5) {
		case 0:
			id := 1 + rng.Intn(5)
			c.crash(id)
			time.Sleep(time.Duration(rng.Intn(300)) * time.Millisecond)
			c.start(id)
		case 1, 2:
			perm := rng.Perm(5)
			cut := 1 + rng.Intn(2)
			var a, b []int
			for i, p := range perm {
				if i < cut {
					a = append(a, p+1)
				} else {
					b = append(b, p+1)
				}
			}
			c.net.Partition(a, b)
		default:
			c.net.Heal()
		}
		time.Sleep(time.Duration(100+rng.Intn(400)) * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	res := lin.Check(rec.ops, 60*time.Second)
	t.Logf("seed %d: %s", seed, res)
	if len(rec.ops) < 200 {
		t.Fatalf("only %d operations completed: the cluster made too little progress", len(rec.ops))
	}
	return res
}

func TestLinearizableUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	for _, mode := range []kv.ReadMode{kv.ReadIndex, kv.ReadLog} {
		for seed := int64(1); seed <= 2; seed++ {
			t.Run(fmt.Sprintf("mode%d/seed%d", mode, seed), func(t *testing.T) {
				res := runWorkload(t, seed, kv.Options{ReadMode: mode, SnapshotEvery: 200}, 5*time.Second)
				if !res.Ok {
					t.Fatal(res)
				}
			})
		}
	}
}

// TestStaleReadIsDetected builds the classic stale-read scenario: the old
// leader is partitioned away, a new leader accepts a write, and a client
// then reads from the old leader. With ReadIndex the read cannot complete
// on the old leader; with unsafe local reads it returns the old value and
// the checker must reject the history.
func TestStaleReadIsDetected(t *testing.T) {
	for _, tc := range []struct {
		mode    kv.ReadMode
		wantLin bool
	}{{kv.ReadLocalUnsafe, false}, {kv.ReadIndex, true}} {
		t.Run(fmt.Sprint("mode", tc.mode), func(t *testing.T) {
			c := newCluster(t, 3, 42, kv.Options{ReadMode: tc.mode})
			rec := &recorder{start: time.Now()}
			cl := newClient(c, rec, 1, 0)
			put := func(v string) {
				op := lin.Op{Kind: lin.Put, Key: "x", Value: v, Call: rec.now()}
				cl.target = c.leader()
				if _, ok := cl.do(kv.Request{Kind: kv.OpPut, Key: "x", Value: v}, 5*time.Second); !ok {
					t.Fatal("put failed")
				}
				op.Return = rec.now()
				rec.add(op)
			}
			put("1")
			old := c.leader()
			// Wait until every node has applied x=1.
			for _, id := range []int{1, 2, 3} {
				for c.store(id).Len() == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
			var rest []int
			for id := 1; id <= 3; id++ {
				if id != old {
					rest = append(rest, id)
				}
			}
			c.net.Partition([]int{old}, rest)
			for c.leader() == old {
				time.Sleep(10 * time.Millisecond)
			}
			put("2")

			// Read directly from the old leader, before it notices.
			op := lin.Op{Kind: lin.Get, Key: "x", Call: rec.now()}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			resp, err := c.store(old).Do(ctx, kv.Request{Kind: kv.OpGet, Key: "x"})
			cancel()
			op.Return = rec.now()
			if err == nil {
				op.Output, op.Found = resp.Value, resp.Found
				rec.add(op)
			}
			res := lin.Check(rec.ops, 10*time.Second)
			t.Logf("read err=%v value=%q -> %s", err, resp.Value, res)
			if res.Ok != tc.wantLin {
				t.Fatalf("linearizable=%v, want %v", res.Ok, tc.wantLin)
			}
			if tc.mode == kv.ReadIndex && err == nil {
				t.Fatal("old leader served a ReadIndex read while partitioned")
			}
		})
	}
}

// TestExactlyOnceAppends drops many replies so clients retry writes that
// were already applied; deduplication must keep each append exactly once.
func TestExactlyOnceAppends(t *testing.T) {
	c := newCluster(t, 3, 7, kv.Options{SnapshotEvery: 50})
	c.leader()
	c.net.SetUnreliable(0.2, 0, 5*time.Millisecond)
	rec := &recorder{start: time.Now()}
	const clients, each = 4, 40
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		cl := newClient(c, rec, int64(i), i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				if _, ok := cl.do(kv.Request{Kind: kv.OpAppend, Key: "log", Value: fmt.Sprintf("<%d.%d>", i, j)}, 20*time.Second); !ok {
					t.Errorf("append %d.%d never completed", i, j)
				}
			}
		}(i)
	}
	wg.Wait()
	c.net.SetUnreliable(0, 0, 0)
	cl := newClient(c, rec, 99, 99)
	resp, ok := cl.do(kv.Request{Kind: kv.OpGet, Key: "log"}, 5*time.Second)
	if !ok {
		t.Fatal("final read failed")
	}
	for i := 0; i < clients; i++ {
		for j := 0; j < each; j++ {
			if n := strings.Count(resp.Value, fmt.Sprintf("<%d.%d>", i, j)); n != 1 {
				t.Fatalf("append %d.%d present %d times", i, j, n)
			}
		}
	}
}
