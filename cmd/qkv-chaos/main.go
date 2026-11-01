// Command qkv-chaos runs a real multi-process cluster under a nemesis
// (network partitions, kill -9, restarts) while concurrent clients issue
// random operations, then checks the recorded history for linearizability.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/eshaan2418/quorumkv/client"
	"github.com/eshaan2418/quorumkv/internal/launch"
	"github.com/eshaan2418/quorumkv/lin"
)

type summary struct {
	Nodes          int            `json:"nodes"`
	Clients        int            `json:"clients"`
	Keys           int            `json:"keys"`
	ReadMode       string         `json:"read_mode"`
	SpreadReads    bool           `json:"spread_reads"`
	DurationS      float64        `json:"duration_s"`
	Seed           int64          `json:"seed"`
	Ops            int            `json:"ops_recorded"`
	OKOps          int            `json:"ops_ok"`
	UnknownOps     int            `json:"writes_unknown_outcome"`
	FailedReads    int            `json:"reads_failed"`
	Faults         map[string]int `json:"faults"`
	Linearizable   bool           `json:"linearizable"`
	CheckerTimeout bool           `json:"checker_timeout"`
	CheckerMS      float64        `json:"checker_ms"`
	Verdict        string         `json:"verdict"`
}

func main() { os.Exit(run()) }

func run() (code int) {
	defer func() {
		if r := recover(); r != nil {
			f, ok := r.(fatal)
			if !ok {
				panic(r)
			}
			fmt.Fprintln(os.Stderr, "qkv-chaos:", f.err)
			code = 1
		}
	}()
	bin := flag.String("bin", "bin/quorumkv", "server binary")
	n := flag.Int("n", 5, "cluster size")
	clients := flag.Int("clients", 10, "concurrent clients")
	keys := flag.Int("keys", 5, "number of keys")
	dur := flag.Duration("duration", 30*time.Second, "workload duration")
	readMode := flag.String("read-mode", "index", "server read mode")
	seed := flag.Int64("seed", time.Now().UnixNano(), "nemesis/workload seed")
	port := flag.Int("port", 17000, "base port")
	out := flag.String("out", "", "write summary JSON here")
	pidFile := flag.String("pidfile", "", "append started PIDs here")
	spread := flag.Bool("spread-reads", true, "send each GET to a random server (follower-read load balancing)")
	flag.Parse()

	dir, err := os.MkdirTemp("", "qkv-chaos-")
	check(err)
	defer os.RemoveAll(dir)
	c, err := launch.Start(launch.Options{Binary: *bin, N: *n, BasePort: *port, Dir: dir, PIDFile: *pidFile,
		Args: []string{"-chaos", "-read-mode", *readMode, "-snapshot-every", "500", "-election-timeout", "200ms"}})
	check(err)
	defer c.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = c.Leader(ctx)
	cancel()
	check(err)

	rng := rand.New(rand.NewSource(*seed))
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	var mu sync.Mutex
	var history []lin.Op
	sum := summary{Nodes: *n, Clients: *clients, Keys: *keys, ReadMode: *readMode, SpreadReads: *spread, Seed: *seed, Faults: map[string]int{}}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(*seed + int64(i) + 1))
			cl := client.New(c.AddrList(), 500*time.Millisecond)
			cl.SpreadReads(*spread)
			for k := 0; ; k++ {
				select {
				case <-stop:
					return
				default:
				}
				op := lin.Op{Client: i, Key: fmt.Sprint("k", r.Intn(*keys))}
				val := fmt.Sprintf("%d.%d ", i, k)
				octx, ocancel := context.WithTimeout(context.Background(), 3*time.Second)
				op.Call = now()
				var err error
				switch r.Intn(3) {
				case 0:
					op.Kind = lin.Get
					op.Output, op.Found, err = cl.Get(octx, op.Key)
				case 1:
					op.Kind, op.Value = lin.Put, val
					err = cl.Put(octx, op.Key, val)
				default:
					op.Kind, op.Value = lin.Append, val
					err = cl.Append(octx, op.Key, val)
				}
				op.Return = now()
				ocancel()
				mu.Lock()
				switch {
				case err == nil:
					history = append(history, op)
					sum.OKOps++
				case errors.Is(err, client.ErrUnknown) && op.Kind != lin.Get:
					op.Return = lin.Pending
					history = append(history, op)
					sum.UnknownOps++
				default:
					sum.FailedReads++
				}
				mu.Unlock()
			}
		}(i)
	}

	end := time.Now().Add(*dur)
	for time.Now().Before(end) {
		time.Sleep(time.Duration(500+rng.Intn(1500)) * time.Millisecond)
		switch rng.Intn(6) {
		case 0, 1:
			perm := rng.Perm(*n)
			cut := 1 + rng.Intn(*n/2)
			var a, b []int
			for i, p := range perm {
				if i < cut {
					a = append(a, p+1)
				} else {
					b = append(b, p+1)
				}
			}
			c.Partition(a, b)
			sum.Faults["partition"]++
			fmt.Printf("%6.1fs partition %v | %v\n", time.Since(start).Seconds(), a, b)
		case 2:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			l, err := c.Leader(ctx)
			cancel()
			if err == nil {
				c.Kill(l)
				sum.Faults["kill_leader"]++
				fmt.Printf("%6.1fs kill -9 leader %d\n", time.Since(start).Seconds(), l)
				time.Sleep(time.Duration(200+rng.Intn(2000)) * time.Millisecond)
				check(c.StartNode(l))
				fmt.Printf("%6.1fs restart %d\n", time.Since(start).Seconds(), l)
			}
		case 3:
			id := 1 + rng.Intn(*n)
			c.Kill(id)
			sum.Faults["kill_random"]++
			fmt.Printf("%6.1fs kill -9 node %d\n", time.Since(start).Seconds(), id)
			time.Sleep(time.Duration(200+rng.Intn(2000)) * time.Millisecond)
			check(c.StartNode(id))
			fmt.Printf("%6.1fs restart %d\n", time.Since(start).Seconds(), id)
		default:
			c.Heal()
			sum.Faults["heal"]++
			fmt.Printf("%6.1fs heal\n", time.Since(start).Seconds())
		}
	}
	c.Heal()
	close(stop)
	wg.Wait()
	sum.DurationS = time.Since(start).Seconds()
	sum.Ops = len(history)

	t0 := time.Now()
	res := lin.Check(history, 5*time.Minute)
	sum.CheckerMS = float64(time.Since(t0).Microseconds()) / 1000
	sum.Linearizable, sum.CheckerTimeout, sum.Verdict = res.Ok, res.Timeout, res.String()
	b, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		check(os.MkdirAll(filepath.Dir(*out), 0o755))
		check(os.WriteFile(*out, append(b, '\n'), 0o644))
	}
	if !res.Ok {
		logs, _ := filepath.Glob(filepath.Join(dir, "node*", "server.log"))
		fmt.Println("server logs (deleted on exit):", strings.Join(logs, " "))
		return 1
	}
	return 0
}

type fatal struct{ err error }

// check aborts run() via panic so deferred cleanup (killing servers,
// removing the temp dir) always executes.
func check(err error) {
	if err != nil {
		panic(fatal{err})
	}
}
