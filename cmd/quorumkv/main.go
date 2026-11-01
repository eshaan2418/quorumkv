// Command quorumkv runs one member of a quorumkv cluster.
//
//	quorumkv -id 1 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data ./data/1
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eshaan2418/quorumkv/kv"
	"github.com/eshaan2418/quorumkv/server"
)

func main() {
	id := flag.Int("id", 0, "this node's id (must appear in -cluster)")
	cluster := flag.String("cluster", "", "comma-separated id=host:port for every member")
	data := flag.String("data", "", "data directory (WAL and snapshots)")
	noSync := flag.Bool("nosync", false, "skip fsync (UNSAFE: benchmarking only)")
	snapEvery := flag.Uint64("snapshot-every", 10000, "log entries between snapshots (0 = never)")
	readMode := flag.String("read-mode", "index", "how GETs are served: index | log | unsafe-local")
	election := flag.Duration("election-timeout", 300*time.Millisecond, "base election timeout")
	chaos := flag.Bool("chaos", false, "enable POST /admin/block for fault injection")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	peers, err := parseCluster(*cluster)
	if err != nil || *id == 0 || *data == "" {
		fmt.Fprintln(os.Stderr, "usage: quorumkv -id N -cluster 1=host:port,... -data DIR")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(2)
	}
	mode, err := kv.ParseReadMode(*readMode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	srv, err := server.New(server.Config{ID: *id, Peers: peers, DataDir: *data, NoSync: *noSync, SnapshotEvery: *snapEvery,
		ReadMode: mode, ElectionTimeout: *election, Chaos: *chaos, Logger: log})
	if err != nil {
		log.Error("start failed", "err", err)
		os.Exit(1)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		srv.Close()
	}()
	log.Info("serving", "id", *id, "addr", peers[*id], "read_mode", *readMode, "nosync", *noSync)
	if err := srv.Serve(); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func parseCluster(s string) (map[int]string, error) {
	peers := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("bad -cluster entry %q", part)
		}
		id, err := strconv.Atoi(k)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("bad node id %q", k)
		}
		peers[id] = v
	}
	return peers, nil
}
