# quorumkv design notes

## Layers

```
cmd/quorumkv ── server (HTTP: /raft/*, /kv, /status, /admin/block)
                  │
                  ├── kv     replicated state machine, sessions, read modes
                  │
                  └── raft   consensus: election, replication, snapshots, ReadIndex
                        │
                        ├── Storage   FileStorage (WAL + snapshot) | MemStorage (tests)
                        └── Transport HTTP+gob | memnet (tests)
```

`raft` knows nothing about keys and values. It delivers `ApplyMsg`s, which are committed entries or installed snapshots, in log order on a channel. `kv` decodes and applies them.

## Raft node

The node uses one mutex. Background goroutines do the work that blocks:

| goroutine | job |
|---|---|
| `ticker` | election timeouts (followers) and CheckQuorum (leader) |
| `replicator(p)` | one in-flight AppendEntries / InstallSnapshot per peer, batching up to 1024 entries |
| `heartbeater(p)` | empty AppendEntries on a fixed schedule, plus on demand for ReadIndex |
| `flusher` | the leader's group commit: one fsync covers every entry appended while the previous fsync ran |
| `applier` | copies committed entries out under the lock and delivers them outside it |

### Safety-relevant decisions

- **Persistence before replies.** Votes, term changes, and follower log appends are fsynced before the RPC reply is sent. The leader counts itself toward a commit majority only up to its own `durable` index, which the flusher advances.
- **Follower fsync without the lock.** A follower writes entries under the lock, releases it to fsync, then re-acquires it. Before acknowledging, it checks that the term and the truncation counter are unchanged. Within one term the leader never rewrites entries it has sent, so an unchanged (term, truncations) pair means the synced entries are exactly the ones being acknowledged. If either changed, it replies with failure and the leader retries. This keeps heartbeats, and therefore ReadIndex confirmations, from waiting behind a multi-millisecond `F_FULLFSYNC`.
- **Truncate only on conflict.** A delayed AppendEntries whose entries are all already present must not truncate newer entries. The handler skips matching entries and truncates only at the first real conflict. Truncating a committed index panics: it can only happen through a bug, and the tests would catch it.
- **Commit only current-term entries by counting** (figure 8 of the Raft paper). A new leader appends a no-op so that entries from earlier terms become committed promptly.
- **Fast backtracking.** Failed AppendEntries replies carry `(ConflictTerm, first index of that term)`, or `ConflictIndex = len` when the follower's log is too short, so the leader skips a whole term per round trip.

### Liveness under partitions

- **PreVote.** Before incrementing its term, a node asks peers whether they would vote for it. Peers refuse if they have heard from a leader within the election timeout, or if the candidate's log is behind. A node that was partitioned away therefore cannot inflate its term and depose a healthy leader when it rejoins. Covered by `TestPreVotePreventsDisruption`.
- **CheckQuorum.** A leader that has not heard from a majority within one election timeout steps down. This handles asymmetric partitions (the leader can send but not receive). It also stops a stale leader from accepting writes that can never commit. Covered by `TestAsymmetricPartitionRecovers`.
- **Leader stickiness.** Real votes are also refused while a live leader is known. CheckQuorum bounds how long a dead leader can keep that protection.

## Reads

| mode | how | cost | linearizable |
|---|---|---|---|
| `index` (default) | ReadIndex: record `commit`, confirm leadership with a heartbeat round acknowledged by a majority, wait until the state machine has applied `commit`, then read locally | one heartbeat RTT, no disk | yes |
| `log` | the Get goes through the log like a write | a full replicated, fsynced write | yes |
| `unsafe-local` | read whatever node you reached | none | **no** |

ReadIndex rounds are numbered. A heartbeat carries the round that was current when it was sent, and a read is released once a majority (counting the leader) has acknowledged a round at least as new as the read's. A partitioned leader therefore cannot confirm a read even before CheckQuorum demotes it (`TestReadIndex`). The commit index is used only after the leader has committed an entry from its own term, which the no-op guarantees.

## Exactly-once writes

Each client has a random 64-bit session id and a sequence number. The state machine stores, per session, the last applied sequence number and its result. A retried write with `Seq <= last` returns the cached result without re-applying it. The client keeps the same `(id, seq)` across all retries, through timeouts, redirects and leader changes, so `Append` is exactly-once (`TestExactlyOnceAppends` drops 20% of messages). Sessions are included in snapshots.

## Storage format

`wal` is a sequence of records `[u32 len][u32 crc32c][u8 type][payload]`, carrying either a hard state or a batch of entries. An entries record whose first index is at or below the current end truncates the suffix, so the WAL replays the in-memory log exactly. Recovery stops at the first torn or corrupt record and truncates the file there. That can only be an unsynced tail, because everything acknowledged was fsynced.

Snapshots are written to `snap.tmp`, fsynced, renamed, and the directory is fsynced. The WAL is then rewritten the same way with only the post-snapshot entries. A crash between the two renames leaves a new snapshot next to an old WAL, and `Load` drops the WAL entries the snapshot already covers (`TestFileStorageCrashBetweenSnapshotAndWALRewrite`).

## Linearizability checker

`lin` implements the Wing & Gong backtracking search with Lowe's memoization of `(set of linearized ops, model state)`, the same algorithm as Porcupine. Histories are split per key, which is valid because linearizability is compositional, and each key's sub-history is checked independently. A write whose client gave up has `Return = +∞`. It may then be linearized anywhere after its call, which includes "never", by placing it after everything observable. The test suite cross-checks the verdicts against Porcupine on 2,000 random histories.

## Known limitations

- No membership changes: the set of peers is fixed at startup.
- One in-flight AppendEntries per follower (no pipelining), so write latency is about two fsyncs plus a round trip.
- Snapshots are sent in one RPC. Very large state would need chunked InstallSnapshot.
- HTTP plus gob is convenient, not fast. A framed binary protocol over long-lived TCP connections would cut per-RPC overhead.
