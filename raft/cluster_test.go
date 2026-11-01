package raft_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eshaan2418/quorumkv/raft"
	"github.com/eshaan2418/quorumkv/transport/memnet"
)

const electionTimeout = 100 * time.Millisecond

// cluster runs n Raft nodes on a memnet and checks State Machine Safety on
// every apply: no two nodes may ever apply different data at the same index.
type cluster struct {
	t         *testing.T
	net       *memnet.Network
	n         int
	snapEvery uint64

	mu      sync.Mutex
	nodes   map[int]*raft.Node
	chans   map[int]chan raft.ApplyMsg
	done    map[int]chan struct{}
	stores  map[int]*raft.MemStorage
	applied map[int]uint64          // last applied index per node
	log     map[uint64][]byte       // index -> data, first applier wins
	count   map[uint64]map[int]bool // index -> nodes that applied it
	failure string
}

func newCluster(t *testing.T, n int, seed int64) *cluster {
	c := &cluster{t: t, net: memnet.New(seed), n: n, nodes: map[int]*raft.Node{}, chans: map[int]chan raft.ApplyMsg{},
		done: map[int]chan struct{}{}, stores: map[int]*raft.MemStorage{}, applied: map[int]uint64{},
		log: map[uint64][]byte{}, count: map[uint64]map[int]bool{}}
	for id := 1; id <= n; id++ {
		c.stores[id] = raft.NewMemStorage()
		c.start(id)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) peers() []int {
	p := make([]int, c.n)
	for i := range p {
		p[i] = i + 1
	}
	return p
}

func (c *cluster) start(id int) {
	ch := make(chan raft.ApplyMsg, 256)
	node, err := raft.New(raft.Config{
		ID: id, Peers: c.peers(), Transport: c.net.Transport(id), Storage: c.stores[id], Apply: ch,
		ElectionTimeout: electionTimeout, HeartbeatInterval: 20 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	done := make(chan struct{})
	c.mu.Lock()
	c.nodes[id], c.chans[id], c.done[id] = node, ch, done
	c.applied[id] = 0
	c.mu.Unlock()
	c.net.Register(id, node)
	go c.consume(id, node, ch, done)
}

func (c *cluster) consume(id int, node *raft.Node, ch chan raft.ApplyMsg, done chan struct{}) {
	defer close(done)
	for m := range ch {
		c.mu.Lock()
		if m.SnapshotValid {
			c.applied[id] = binary.LittleEndian.Uint64(m.Snapshot.Data)
			if c.applied[id] != m.Snapshot.Index {
				c.fail("node %d: snapshot data %d != index %d", id, c.applied[id], m.Snapshot.Index)
			}
			c.mu.Unlock()
			continue
		}
		if m.Index != c.applied[id]+1 {
			c.fail("node %d applied %d after %d", id, m.Index, c.applied[id])
		}
		c.applied[id] = m.Index
		if prev, ok := c.log[m.Index]; ok && !bytes.Equal(prev, m.Data) {
			c.fail("SAFETY: node %d applied %q at %d, another node applied %q", id, m.Data, m.Index, prev)
		} else if !ok {
			c.log[m.Index] = m.Data
		}
		if c.count[m.Index] == nil {
			c.count[m.Index] = map[int]bool{}
		}
		c.count[m.Index][id] = true
		snap := c.snapEvery > 0 && m.Index%c.snapEvery == 0
		c.mu.Unlock()
		if snap {
			node.Snapshot(m.Index, binary.LittleEndian.AppendUint64(nil, m.Index))
		}
	}
}

func (c *cluster) fail(format string, args ...any) {
	if c.failure == "" {
		c.failure = fmt.Sprintf(format, args...)
	}
}

func (c *cluster) check() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != "" {
		c.t.Fatal(c.failure)
	}
}

func (c *cluster) crash(id int) {
	c.mu.Lock()
	node, ch, done := c.nodes[id], c.chans[id], c.done[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if node == nil {
		return
	}
	c.net.Unregister(id)
	node.Stop()
	close(ch)
	<-done
}

func (c *cluster) shutdown() {
	for id := 1; id <= c.n; id++ {
		c.crash(id)
	}
}

func (c *cluster) live() map[int]*raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int]*raft.Node{}
	for k, v := range c.nodes {
		out[k] = v
	}
	return out
}

// leader waits for a leader among `among` (default: all live) and returns it.
func (c *cluster) leader(among ...int) int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		best, bestTerm := 0, uint64(0)
		for id, n := range c.live() {
			if len(among) > 0 && !contains(among, id) {
				continue
			}
			s := n.Status()
			if s.Role == raft.Leader && s.Term >= bestTerm {
				best, bestTerm = id, s.Term
			}
		}
		if best != 0 {
			return best
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected among %v", among)
	return 0
}

// one proposes data until some leader commits it and at least `want` nodes
// have applied it, returning the index.
func (c *cluster) one(data []byte, want int) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.live() {
			idx, _, err := n.Propose(data)
			if err != nil {
				continue
			}
			wait := time.Now().Add(2 * time.Second)
			for time.Now().Before(wait) {
				c.mu.Lock()
				got, ok := c.log[idx]
				cnt := len(c.count[idx])
				c.mu.Unlock()
				if ok && cnt >= want && bytes.Equal(got, data) {
					return idx
				}
				if ok && !bytes.Equal(got, data) {
					break // a different entry won this index; retry
				}
				time.Sleep(5 * time.Millisecond)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("failed to commit %q on %d nodes", data, want)
	return 0
}

func (c *cluster) waitApplied(id int, idx uint64) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		a := c.applied[id]
		c.mu.Unlock()
		if a >= idx {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %d did not apply index %d", id, idx)
}

func contains(xs []int, x int) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func others(n, except int) []int {
	var out []int
	for i := 1; i <= n; i++ {
		if i != except {
			out = append(out, i)
		}
	}
	return out
}

// ---------------------------------------------------------------------------

func TestInitialElectionIsStable(t *testing.T) {
	c := newCluster(t, 3, 1)
	l := c.leader()
	term := c.nodes[l].Status().Term
	time.Sleep(10 * electionTimeout)
	if l2 := c.leader(); l2 != l || c.nodes[l2].Status().Term != term {
		t.Fatalf("leadership changed without faults: %d@%d -> %d@%d", l, term, l2, c.nodes[l2].Status().Term)
	}
	c.check()
}

func TestSingleNode(t *testing.T) {
	c := newCluster(t, 1, 1)
	c.leader()
	for i := 0; i < 20; i++ {
		c.one([]byte(fmt.Sprint(i)), 1)
	}
	idx, err := c.nodes[1].ReadIndex(context.Background())
	if err != nil || idx < 20 {
		t.Fatalf("ReadIndex = %d, %v", idx, err)
	}
	c.check()
}

func TestReElectionAfterLeaderPartition(t *testing.T) {
	c := newCluster(t, 3, 2)
	old := c.leader()
	c.one([]byte("a"), 3)
	c.net.Partition([]int{old}, others(3, old))
	nl := c.leader(others(3, old)...)
	if nl == old {
		t.Fatal("isolated node still leader")
	}
	c.one([]byte("b"), 2)
	c.net.Heal()
	c.one([]byte("c"), 3)
	// Exactly one leader in the highest term.
	leaders := 0
	for _, n := range c.live() {
		if n.Status().Role == raft.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders after heal", leaders)
	}
	c.check()
}

func TestNoProgressWithoutQuorum(t *testing.T) {
	c := newCluster(t, 5, 3)
	l := c.leader()
	c.one([]byte("x"), 5)
	minority := []int{l, others(5, l)[0]}
	c.net.Partition(minority, others5Except(minority))
	c.leader(others5Except(minority)...)
	time.Sleep(5 * electionTimeout)
	// The minority side must not commit anything new.
	before := c.nodes[l].Status().Commit
	for _, id := range minority {
		c.nodes[id].Propose([]byte("lost"))
	}
	time.Sleep(5 * electionTimeout)
	for _, id := range minority {
		if s := c.nodes[id].Status(); s.Commit > before {
			t.Fatalf("minority node %d advanced commit to %d", id, s.Commit)
		}
		if s := c.nodes[id].Status(); s.Role == raft.Leader {
			t.Fatalf("minority node %d is leader (CheckQuorum failed)", id)
		}
	}
	c.net.Heal()
	c.one([]byte("y"), 5)
	c.check()
}

func others5Except(xs []int) []int {
	var out []int
	for i := 1; i <= 5; i++ {
		if !contains(xs, i) {
			out = append(out, i)
		}
	}
	return out
}

func TestFollowerCatchUp(t *testing.T) {
	c := newCluster(t, 3, 4)
	l := c.leader()
	f := others(3, l)[0]
	c.net.Partition([]int{f}, others(3, f))
	var last uint64
	for i := 0; i < 300; i++ {
		last = c.one([]byte(fmt.Sprint("e", i)), 2)
	}
	c.net.Heal()
	c.waitApplied(f, last)
	c.check()
}

func TestDivergentLogIsOverwritten(t *testing.T) {
	c := newCluster(t, 5, 5)
	old := c.leader()
	c.one([]byte("base"), 5)
	c.net.Partition([]int{old}, others(5, old))
	for i := 0; i < 20; i++ {
		c.nodes[old].Propose([]byte(fmt.Sprint("doomed", i)))
	}
	c.leader(others(5, old)...)
	for i := 0; i < 20; i++ {
		c.one([]byte(fmt.Sprint("kept", i)), 4)
	}
	c.net.Heal()
	idx := c.one([]byte("final"), 5)
	c.waitApplied(old, idx)
	c.mu.Lock()
	for i := uint64(1); i <= idx; i++ {
		if bytes.HasPrefix(c.log[i], []byte("doomed")) {
			t.Errorf("uncommitted entry from deposed leader was applied at %d", i)
		}
	}
	c.mu.Unlock()
	c.check()
}

func TestPreVotePreventsDisruption(t *testing.T) {
	c := newCluster(t, 3, 6)
	l := c.leader()
	c.one([]byte("a"), 3)
	term := c.nodes[l].Status().Term
	f := others(3, l)[0]
	c.net.Partition([]int{f}, others(3, f))
	time.Sleep(10 * electionTimeout) // f times out repeatedly
	if ft := c.nodes[f].Status().Term; ft != term {
		t.Fatalf("partitioned follower bumped its term %d -> %d despite PreVote", term, ft)
	}
	c.net.Heal()
	c.one([]byte("b"), 3)
	if s := c.nodes[l].Status(); s.Role != raft.Leader || s.Term != term {
		t.Fatalf("rejoining follower disrupted the leader: role=%v term=%d", s.Role, s.Term)
	}
	c.check()
}

func TestAsymmetricPartitionRecovers(t *testing.T) {
	// The leader can send but not receive: CheckQuorum must make it step down.
	c := newCluster(t, 3, 7)
	l := c.leader()
	for _, p := range others(3, l) {
		c.net.Cut(p, l)
	}
	deadline := time.Now().Add(3 * time.Second)
	for c.nodes[l].Status().Role == raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("leader that cannot hear from followers did not step down")
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.net.Heal()
	c.one([]byte("after"), 3)
	c.check()
}

func TestSnapshotInstall(t *testing.T) {
	c := newCluster(t, 3, 8)
	c.snapEvery = 25
	l := c.leader()
	f := others(3, l)[0]
	c.net.Partition([]int{f}, others(3, f))
	var last uint64
	for i := 0; i < 200; i++ {
		last = c.one([]byte(fmt.Sprint("s", i)), 2)
	}
	if c.nodes[l].Status().SnapshotIndex == 0 {
		t.Fatal("leader never compacted its log")
	}
	c.net.Heal()
	c.waitApplied(f, last)
	if c.nodes[f].Status().SnapshotIndex == 0 {
		t.Fatal("lagging follower caught up without InstallSnapshot")
	}
	c.one([]byte("post"), 3)
	c.check()
}

func TestRestartAll(t *testing.T) {
	c := newCluster(t, 3, 9)
	c.snapEvery = 10
	for i := 0; i < 35; i++ {
		c.one([]byte(fmt.Sprint("r", i)), 3)
	}
	for id := 1; id <= 3; id++ {
		c.crash(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	idx := c.one([]byte("after-restart"), 3)
	if idx < 36 {
		t.Fatalf("log was lost across restart: new entry at %d", idx)
	}
	c.check()
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3, 10)
	l := c.leader()
	idx := c.one([]byte("w"), 3)
	ri, err := c.nodes[l].ReadIndex(context.Background())
	if err != nil || ri < idx {
		t.Fatalf("ReadIndex = %d, %v (want >= %d)", ri, err, idx)
	}
	f := others(3, l)[0]
	if _, err := c.nodes[f].ReadIndex(context.Background()); err != raft.ErrNotLeader {
		t.Fatalf("follower ReadIndex err = %v", err)
	}
	// An isolated leader must not confirm a read, even before it steps down.
	c.net.Partition([]int{l}, others(3, l))
	ctx, cancel := context.WithTimeout(context.Background(), 3*electionTimeout)
	defer cancel()
	if _, err := c.nodes[l].ReadIndex(ctx); err == nil {
		t.Fatal("isolated leader confirmed a ReadIndex")
	}
	c.check()
}

// TestChaos runs proposals while crashing, restarting and partitioning nodes
// over an unreliable network, then heals and checks every node converges.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			c := newCluster(t, 5, seed*100)
			c.snapEvery = 50
			c.net.SetUnreliable(0.1, 0, 15*time.Millisecond)
			rng := newRand(seed)
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				i := 0
				for {
					select {
					case <-stop:
						return
					default:
					}
					for _, n := range c.live() {
						n.Propose([]byte(fmt.Sprint("c", i)))
					}
					i++
					time.Sleep(2 * time.Millisecond)
				}
			}()
			end := time.Now().Add(4 * time.Second)
			for time.Now().Before(end) {
				switch rng.Intn(4) {
				case 0:
					id := 1 + rng.Intn(5)
					c.crash(id)
					time.Sleep(time.Duration(rng.Intn(300)) * time.Millisecond)
					c.start(id)
				case 1:
					perm := rng.Perm(5)
					var a, b []int
					for i, p := range perm {
						if i < 2 {
							a = append(a, p+1)
						} else {
							b = append(b, p+1)
						}
					}
					c.net.Partition(a, b)
				default:
					c.net.Heal()
				}
				time.Sleep(time.Duration(50+rng.Intn(400)) * time.Millisecond)
			}
			close(stop)
			wg.Wait()
			c.net.Heal()
			c.net.SetUnreliable(0, 0, 0)
			for id := 1; id <= 5; id++ {
				if _, ok := c.live()[id]; !ok {
					c.start(id)
				}
			}
			idx := c.one([]byte("final"), 5)
			for id := 1; id <= 5; id++ {
				c.waitApplied(id, idx)
			}
			c.check()
		})
	}
}
