// Package raft implements the Raft consensus protocol: leader election with
// PreVote and CheckQuorum, log replication with fast conflict backtracking,
// durable state with group-committed fsync, snapshots, and ReadIndex for
// linearizable reads without writing to the log.
package raft

import (
	"context"
	"errors"
)

// Entry is one log entry. A nil Data marks the no-op a new leader appends to
// commit entries from earlier terms.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// HardState is the state Raft must persist before responding to RPCs.
type HardState struct {
	Term uint64
	Vote int // 0 = none; node IDs start at 1
}

// Snapshot is a compacted prefix of the log.
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type RequestVoteArgs struct {
	Term         uint64
	Candidate    int
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
}

type RequestVoteReply struct {
	Term    uint64
	Granted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	Leader       int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// On success, the highest index known to match the leader's log.
	MatchIndex uint64
	// On failure, hints for fast backtracking (Raft paper, section 5.3).
	ConflictIndex uint64
	ConflictTerm  uint64
}

type InstallSnapshotArgs struct {
	Term              uint64
	Leader            int
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Transport delivers RPCs to peers. Implementations must be safe for
// concurrent use and should honour ctx cancellation.
type Transport interface {
	RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler is the receiving side of Transport; *Node implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}

// ApplyMsg is delivered, in log order, on Config.Apply.
type ApplyMsg struct {
	// Exactly one of Command or Snapshot is set.
	CommandValid bool
	Index        uint64
	Term         uint64
	Data         []byte // nil for leader no-ops

	SnapshotValid bool
	Snapshot      Snapshot
}

var (
	ErrNotLeader = errors.New("raft: not the leader")
	ErrStopped   = errors.New("raft: node stopped")
)
