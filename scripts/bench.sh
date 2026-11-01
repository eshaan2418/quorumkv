#!/bin/sh
# Reproduces bench/results/: load matrix, failover timing, fsync floor.
# Every load run starts a fresh cluster; clients and servers share the machine.
set -eu
OUT=${OUT:-bench/results}
PIDS=${PIDS:-/dev/null}
export GOMAXPROCS=${GOMAXPROCS:-9}
mkdir -p "$OUT"
rm -f "$OUT/load.jsonl" "$OUT/failover.jsonl"

load() {
  name=$1; shift
  ./bin/qkv-bench -name "$name" -clients 32 -duration 10s -warmup 2s -runs 5 -value-size 128 \
    -out "$OUT/load.jsonl" -pidfile "$PIDS" "$@" | grep ' run '
}

load "1 node, fsync"                    -n 1 -reads 0.5
load "3 nodes, fsync"                   -n 3 -reads 0.5
load "5 nodes, fsync"                   -n 5 -reads 0.5
load "3 nodes, no fsync (unsafe)"       -n 3 -reads 0.5 -args "-nosync"
load "3 nodes, 90% reads, ReadIndex"    -n 3 -reads 0.9 -args "-read-mode index"
load "3 nodes, 90% reads, reads in log" -n 3 -reads 0.9 -args "-read-mode log"

for n in 3 5; do
  ./bin/qkv-bench -name "failover, $n nodes" -n $n -failover 10 -out "$OUT/failover.jsonl" -pidfile "$PIDS" | grep failover
done

go test -run '^$' -bench FileStorageAppendSync -benchtime 200x -count 5 ./raft/ | grep -E 'Benchmark' > "$OUT/fsync.txt"
