#!/bin/sh
# Runs qkv-chaos for several seeds per read mode and writes one JSON summary
# per run to bench/results/chaos/. Exits non-zero if a safe mode ever fails.
set -u
OUT=${OUT:-bench/results/chaos}
DUR=${DUR:-30s}
SEEDS=${SEEDS:-"1 2 3 4 5"}
PIDS=${PIDS:-/dev/null}
export GOMAXPROCS=${GOMAXPROCS:-9}
mkdir -p "$OUT"
fail=0
for mode in index log; do
  for s in $SEEDS; do
    ./bin/qkv-chaos -n 5 -clients 10 -keys 5 -duration "$DUR" -seed "$s" -read-mode $mode \
      -out "$OUT/$mode-seed$s.json" -pidfile "$PIDS" > /dev/null || fail=1
    printf '%-6s seed %s: %s\n' $mode "$s" "$(grep '"verdict"' "$OUT/$mode-seed$s.json" | cut -d'"' -f4)"
  done
done
# Control: unsafe reads are expected to be caught at least sometimes.
for s in $SEEDS 6 7 8 9 10; do
  ./bin/qkv-chaos -n 5 -clients 10 -keys 5 -duration 20s -seed "$s" -read-mode unsafe-local \
    -out "$OUT/unsafe-seed$s.json" -pidfile "$PIDS" > /dev/null
  printf '%-6s seed %s: %s\n' unsafe "$s" "$(grep '"verdict"' "$OUT/unsafe-seed$s.json" | cut -d'"' -f4)"
done
exit $fail
