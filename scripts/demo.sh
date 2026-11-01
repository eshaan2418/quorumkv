#!/bin/sh
# Starts a 3-node cluster on localhost, writes and reads through it, kills the
# leader with SIGKILL, and shows the cluster keeps serving.
set -eu
DIR=$(mktemp -d)
trap 'kill $(cat "$DIR"/pids) 2>/dev/null || true; rm -rf "$DIR"' EXIT
CLUSTER=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
for i in 1 2 3; do
  # Subshell: the server is not a job of this shell, so killing it later
  # does not print a job-control message.
  ( ./bin/quorumkv -id $i -cluster $CLUSTER -data "$DIR/n$i" 2> "$DIR/n$i.log" & echo $! >> "$DIR/pids" )
done
sleep 1.5
leader() {
  for i in 1 2 3; do
    curl -s 127.0.0.1:700$i/status 2>/dev/null | grep -q '"role":"leader"' && { echo $i; return; }
  done
}
echo "\$ qkv put greeting hello"; ./bin/qkv put greeting hello
echo "\$ qkv append greeting ', world'"; ./bin/qkv append greeting ', world'
echo "\$ qkv get greeting"; ./bin/qkv get greeting
L=$(leader); echo "# leader is node $L; kill -9 it"
kill -9 "$(sed -n ${L}p "$DIR/pids")"
echo "\$ qkv put after-failover yes"
start=$(python3 -c 'import time; print(time.time())')
./bin/qkv put after-failover yes
python3 -c "import time; print('# write acknowledged %.0f ms after the kill' % ((time.time() - $start) * 1000))"
echo "\$ qkv get greeting"; ./bin/qkv get greeting
echo "# new leader: node $(leader)"
