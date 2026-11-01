package server_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/eshaan2418/quorumkv/client"
	"github.com/eshaan2418/quorumkv/server"
)

func freePorts(t *testing.T, n int) map[int]string {
	peers := map[int]string{}
	var lns []net.Listener
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns = append(lns, ln)
		peers[i] = ln.Addr().String()
	}
	for _, ln := range lns {
		ln.Close()
	}
	return peers
}

type proc struct {
	srv  *server.Server
	done chan error
}

func startServer(t *testing.T, id int, peers map[int]string, dir string) *proc {
	t.Helper()
	srv, err := server.New(server.Config{ID: id, Peers: peers, DataDir: filepath.Join(dir, fmt.Sprint(id)),
		SnapshotEvery: 50, ElectionTimeout: 150 * time.Millisecond, Chaos: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	p := &proc{srv: srv, done: make(chan error, 1)}
	go func() { p.done <- srv.Serve() }()
	return p
}

func (p *proc) stop() {
	p.srv.Close()
	<-p.done
}

func TestClusterOverHTTPWithRestart(t *testing.T) {
	peers := freePorts(t, 3)
	dir := t.TempDir()
	procs := map[int]*proc{}
	for id := 1; id <= 3; id++ {
		procs[id] = startServer(t, id, peers, dir)
	}
	defer func() {
		for _, p := range procs {
			p.stop()
		}
	}()
	addrs := []string{peers[1], peers[2], peers[3]}
	cl := client.New(addrs, 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 120; i++ {
		if err := cl.Append(ctx, "log", fmt.Sprintf("%d,", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Find and stop the leader; the cluster must keep serving.
	leader := 0
	for id, addr := range peers {
		if st := status(t, addr); st.Role == "leader" {
			leader = id
		}
	}
	if leader == 0 {
		t.Fatal("no leader")
	}
	procs[leader].stop()
	delete(procs, leader)
	if err := cl.Put(ctx, "after", "failover"); err != nil {
		t.Fatal(err)
	}
	// Restart it from disk; it must recover its snapshot and log and catch up.
	procs[leader] = startServer(t, leader, peers, dir)
	if err := cl.Put(ctx, "final", "x"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := status(t, peers[leader])
		if st.Keys == 3 && st.Snapshot > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
	v, found, err := cl.Get(ctx, "log")
	if err != nil || !found {
		t.Fatal(err)
	}
	want := ""
	for i := 0; i < 120; i++ {
		want += fmt.Sprintf("%d,", i)
	}
	if v != want {
		t.Fatalf("log value mismatch:\n got %q\nwant %q", v, want)
	}
}

func status(t *testing.T, addr string) server.StatusResponse {
	t.Helper()
	st, _ := server.FetchStatus(addr) // a down node reports the zero value
	return st
}
