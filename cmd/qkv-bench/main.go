// Command qkv-bench measures a local multi-process cluster: closed-loop
// throughput and latency percentiles, or leader-failover unavailability.
// Every run starts a fresh cluster in a temp directory.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/eshaan2418/quorumkv/client"
	"github.com/eshaan2418/quorumkv/internal/launch"
)

type runResult struct {
	OpsPerSec  float64 `json:"ops_per_sec"`
	WriteP50us float64 `json:"write_p50_us"`
	WriteP99us float64 `json:"write_p99_us"`
	ReadP50us  float64 `json:"read_p50_us"`
	ReadP99us  float64 `json:"read_p99_us"`
	Errors     int     `json:"errors"`
	Ops        int     `json:"ops"`
}

type report struct {
	Name             string      `json:"name"`
	Nodes            int         `json:"nodes"`
	Clients          int         `json:"clients"`
	ReadRatio        float64     `json:"read_ratio"`
	ValueSize        int         `json:"value_size"`
	ServerArgs       string      `json:"server_args"`
	DurationS        float64     `json:"duration_s"`
	Runs             []runResult `json:"runs"`
	Median           runResult   `json:"median"`
	FailoverMS       []float64   `json:"failover_ms,omitempty"`
	FailoverMedianMS float64     `json:"failover_median_ms,omitempty"`
}

func main() { os.Exit(run()) }

func run() int {
	bin := flag.String("bin", "bin/quorumkv", "server binary")
	name := flag.String("name", "", "label for this configuration")
	n := flag.Int("n", 3, "cluster size")
	clients := flag.Int("clients", 32, "concurrent closed-loop clients")
	dur := flag.Duration("duration", 10*time.Second, "measured duration per run")
	warm := flag.Duration("warmup", 2*time.Second, "warmup per run (not measured)")
	reads := flag.Float64("reads", 0.5, "fraction of operations that are GETs")
	vsize := flag.Int("value-size", 128, "value size in bytes")
	keys := flag.Int("keys", 10000, "key space size")
	runs := flag.Int("runs", 5, "repetitions (fresh cluster each)")
	args := flag.String("args", "", "extra server flags, space-separated")
	failover := flag.Int("failover", 0, "instead of load: kill the leader this many times and time recovery")
	port := flag.Int("port", 18000, "base port")
	out := flag.String("out", "", "append JSON report to this file (JSON lines)")
	pidFile := flag.String("pidfile", "", "append started PIDs here")
	flag.Parse()

	rep := report{Name: *name, Nodes: *n, Clients: *clients, ReadRatio: *reads, ValueSize: *vsize, ServerArgs: *args,
		DurationS: dur.Seconds()}
	extra := strings.Fields(*args)
	start := func() (*launch.Cluster, func(), error) {
		dir, err := os.MkdirTemp("", "qkv-bench-")
		if err != nil {
			return nil, nil, err
		}
		c, err := launch.Start(launch.Options{Binary: *bin, N: *n, BasePort: *port, Dir: dir, Args: extra, PIDFile: *pidFile})
		if err != nil {
			os.RemoveAll(dir)
			return nil, nil, err
		}
		cleanup := func() { c.Stop(); os.RemoveAll(dir) }
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := c.Leader(ctx); err != nil {
			cleanup()
			return nil, nil, err
		}
		return c, cleanup, nil
	}

	if *failover > 0 {
		c, cleanup, err := start()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer cleanup()
		for i := 0; i < *failover; i++ {
			ms, err := measureFailover(c)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			rep.FailoverMS = append(rep.FailoverMS, ms)
			fmt.Printf("failover %d: %.1f ms\n", i+1, ms)
		}
		rep.FailoverMedianMS = median(rep.FailoverMS)
	} else {
		for r := 0; r < *runs; r++ {
			c, cleanup, err := start()
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			res := load(c, *clients, *warm, *dur, *reads, *vsize, *keys, int64(r))
			cleanup()
			rep.Runs = append(rep.Runs, res)
			fmt.Printf("%s run %d: %.0f ops/s  write p50 %.0fus p99 %.0fus  read p50 %.0fus p99 %.0fus  errors %d\n",
				*name, r+1, res.OpsPerSec, res.WriteP50us, res.WriteP99us, res.ReadP50us, res.ReadP99us, res.Errors)
		}
		pick := func(f func(runResult) float64) float64 {
			var xs []float64
			for _, r := range rep.Runs {
				xs = append(xs, f(r))
			}
			return median(xs)
		}
		rep.Median = runResult{
			OpsPerSec:  pick(func(r runResult) float64 { return r.OpsPerSec }),
			WriteP50us: pick(func(r runResult) float64 { return r.WriteP50us }),
			WriteP99us: pick(func(r runResult) float64 { return r.WriteP99us }),
			ReadP50us:  pick(func(r runResult) float64 { return r.ReadP50us }),
			ReadP99us:  pick(func(r runResult) float64 { return r.ReadP99us }),
		}
	}
	b, _ := json.Marshal(rep)
	if *out != "" {
		if err := appendLine(*out, b); err != nil {
			fmt.Fprintln(os.Stderr, "qkv-bench:", err)
			return 1
		}
	}
	fmt.Println(string(b))
	return 0
}

func load(c *launch.Cluster, clients int, warm, dur time.Duration, reads float64, vsize, keys int, seed int64) runResult {
	value := strings.Repeat("v", vsize)
	var mu sync.Mutex
	var wlat, rlat []time.Duration
	var errs int
	measureFrom := time.Now().Add(warm)
	end := measureFrom.Add(dur)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*1000 + int64(i)))
			cl := client.New(c.AddrList(), time.Second)
			var w, r []time.Duration
			e := 0
			for {
				t0 := time.Now()
				if t0.After(end) {
					break
				}
				key := fmt.Sprint("key", rng.Intn(keys))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				isRead := rng.Float64() < reads
				var err error
				if isRead {
					_, _, err = cl.Get(ctx, key)
				} else {
					err = cl.Put(ctx, key, value)
				}
				cancel()
				lat := time.Since(t0)
				if t0.Before(measureFrom) {
					continue
				}
				switch {
				case err != nil:
					e++
				case isRead:
					r = append(r, lat)
				default:
					w = append(w, lat)
				}
			}
			mu.Lock()
			wlat, rlat, errs = append(wlat, w...), append(rlat, r...), errs+e
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	ops := len(wlat) + len(rlat)
	return runResult{
		OpsPerSec:  float64(ops) / dur.Seconds(),
		WriteP50us: pct(wlat, 0.50), WriteP99us: pct(wlat, 0.99),
		ReadP50us: pct(rlat, 0.50), ReadP99us: pct(rlat, 0.99),
		Errors: errs, Ops: ops,
	}
}

// measureFailover kills the leader while a probe client writes continuously
// and returns the time from the kill until the next acknowledged write.
func measureFailover(c *launch.Cluster) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		return 0, err
	}
	cl := client.New(c.AddrList(), 50*time.Millisecond)
	if err := cl.Put(ctx, "probe", "warm"); err != nil {
		return 0, err
	}
	killed := time.Now()
	c.Kill(leader)
	for i := 0; ; i++ {
		if err := cl.Put(ctx, "probe", fmt.Sprint(i)); err == nil {
			break
		}
		if ctx.Err() != nil {
			return 0, fmt.Errorf("no write acknowledged within 10s of killing the leader")
		}
	}
	ms := float64(time.Since(killed).Microseconds()) / 1000
	if err := c.StartNode(leader); err != nil {
		return 0, err
	}
	time.Sleep(2 * time.Second) // let the restarted node catch up
	return ms, nil
}

func pct(xs []time.Duration, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	return float64(xs[int(p*float64(len(xs)-1))].Microseconds())
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func appendLine(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
