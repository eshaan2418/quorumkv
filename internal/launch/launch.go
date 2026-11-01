// Package launch runs a quorumkv cluster as real local processes for chaos
// testing and benchmarks: start, kill -9, restart, and partition via the
// servers' /admin/block endpoint.
package launch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/eshaan2418/quorumkv/server"
)

type Options struct {
	Binary   string // path to the quorumkv server binary
	N        int
	BasePort int
	Dir      string   // data and logs live under Dir/node<i>
	Args     []string // extra server flags
	PIDFile  string   // optional: append started PIDs (one per line)
}

type Cluster struct {
	opts  Options
	Addrs map[int]string
	mu    sync.Mutex
	procs map[int]*exec.Cmd
}

func Start(opts Options) (*Cluster, error) {
	c := &Cluster{opts: opts, Addrs: map[int]string{}, procs: map[int]*exec.Cmd{}}
	for i := 1; i <= opts.N; i++ {
		c.Addrs[i] = fmt.Sprintf("127.0.0.1:%d", opts.BasePort+i)
		if ln, err := net.Listen("tcp", c.Addrs[i]); err != nil {
			return nil, fmt.Errorf("port %s busy: %w", c.Addrs[i], err)
		} else {
			ln.Close()
		}
	}
	for i := 1; i <= opts.N; i++ {
		if err := c.StartNode(i); err != nil {
			c.Stop()
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) clusterFlag() string {
	var parts []string
	for i := 1; i <= c.opts.N; i++ {
		parts = append(parts, fmt.Sprintf("%d=%s", i, c.Addrs[i]))
	}
	return strings.Join(parts, ",")
}

func (c *Cluster) StartNode(id int) error {
	dir := filepath.Join(c.opts.Dir, fmt.Sprintf("node%d", id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	logf, err := os.OpenFile(filepath.Join(dir, "server.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	args := append([]string{"-id", fmt.Sprint(id), "-cluster", c.clusterFlag(), "-data", filepath.Join(dir, "data")}, c.opts.Args...)
	cmd := exec.Command(c.opts.Binary, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	if c.opts.PIDFile != "" {
		if f, err := os.OpenFile(c.opts.PIDFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			_, _ = fmt.Fprintf(f, "%d quorumkv node%d\n", cmd.Process.Pid, id) // best-effort bookkeeping
			f.Close()
		}
	}
	go func() {
		_ = cmd.Wait() // exit status is irrelevant: nodes are killed on purpose
		logf.Close()
	}()
	c.mu.Lock()
	c.procs[id] = cmd
	c.mu.Unlock()
	return c.waitListening(id, 10*time.Second)
}

func (c *Cluster) waitListening(id int, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", c.Addrs[id], 100*time.Millisecond); err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("node %d did not start listening", id)
}

// Kill sends SIGKILL: no clean shutdown, unsynced data is lost.
func (c *Cluster) Kill(id int) {
	c.mu.Lock()
	cmd := c.procs[id]
	delete(c.procs, id)
	c.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGKILL) // fails only if it already exited
		for i := 0; i < 100; i++ {              // wait for the port to be released
			if ln, err := net.Listen("tcp", c.Addrs[id]); err == nil {
				ln.Close()
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (c *Cluster) Running(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.procs[id] != nil
}

func (c *Cluster) Stop() {
	for id := 1; id <= c.opts.N; id++ {
		c.Kill(id)
	}
}

var httpc = &http.Client{Timeout: time.Second}

// Block makes node id drop all Raft traffic to and from peers.
func (c *Cluster) Block(id int, peers []int) error {
	body, _ := json.Marshal(map[string][]int{"peers": peers})
	resp, err := httpc.Post("http://"+c.Addrs[id]+"/admin/block", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Partition splits the cluster into the given groups (best effort: nodes
// that are down are skipped).
func (c *Cluster) Partition(groups ...[]int) {
	group := map[int]int{}
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi
		}
	}
	for id := 1; id <= c.opts.N; id++ {
		var block []int
		for other := 1; other <= c.opts.N; other++ {
			if gi, ok := group[id]; ok {
				if go_, ok2 := group[other]; ok2 && go_ != gi {
					block = append(block, other)
				}
			}
		}
		if c.Running(id) {
			_ = c.Block(id, block) // a node that is restarting misses this fault; acceptable
		}
	}
}

func (c *Cluster) Heal() {
	for id := 1; id <= c.opts.N; id++ {
		if c.Running(id) {
			_ = c.Block(id, nil)
		}
	}
}

func (c *Cluster) Status(id int) (server.StatusResponse, error) {
	return server.FetchStatus(c.Addrs[id])
}

// Leader waits for a node that reports itself leader.
func (c *Cluster) Leader(ctx context.Context) (int, error) {
	for {
		best, bestTerm := 0, uint64(0)
		for id := 1; id <= c.opts.N; id++ {
			if !c.Running(id) {
				continue
			}
			if st, err := c.Status(id); err == nil && st.Role == "leader" && st.Term >= bestTerm {
				best, bestTerm = id, st.Term
			}
		}
		if best != 0 {
			return best, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (c *Cluster) AddrList() []string {
	ids := make([]int, 0, len(c.Addrs))
	for id := range c.Addrs {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var out []string
	for _, id := range ids {
		out = append(out, c.Addrs[id])
	}
	return out
}
