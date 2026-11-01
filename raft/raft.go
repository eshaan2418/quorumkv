package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[r]
}

type Config struct {
	ID        int
	Peers     []int // all members, including ID
	Transport Transport
	Storage   Storage
	Apply     chan<- ApplyMsg

	ElectionTimeout   time.Duration // randomized in [T, 2T)
	HeartbeatInterval time.Duration
	RPCTimeout        time.Duration
	MaxAppendEntries  int
	Logger            *slog.Logger
}

func (c *Config) defaults() {
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = c.ElectionTimeout / 6
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.ElectionTimeout
	}
	if c.MaxAppendEntries == 0 {
		c.MaxAppendEntries = 1024
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// Node is one Raft member. All exported methods are safe for concurrent use.
type Node struct {
	cfg    Config
	id     int
	others []int
	quorum int

	mu   sync.Mutex
	term uint64
	vote int
	role Role
	lead int

	// log[0] is a sentinel holding the snapshot's index and term.
	log      []Entry
	snapshot Snapshot // latest snapshot, kept to send to lagging followers

	commit      uint64
	applied     uint64 // handed to the applier
	durable     uint64 // highest index fsynced locally
	truncs      uint64 // incremented on every truncation (invalidates in-flight syncs)
	pendingSnap *Snapshot

	electionDeadline time.Time
	leaderContact    time.Time // last time we heard from a valid leader

	next, match map[int]uint64
	lastAck     map[int]time.Time
	// ReadIndex: heartbeat rounds acknowledged per peer.
	readRound   uint64
	peerRound   map[int]uint64
	readWaiters []readWaiter

	rng     *rand.Rand
	nudge   map[int]chan struct{}
	hbNudge map[int]chan struct{}
	flushCh chan struct{}
	applyCh chan struct{}
	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
}

type readWaiter struct {
	round uint64
	ch    chan struct{}
}

// New creates a node from persisted state and starts its goroutines.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	st, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:       cfg,
		id:        cfg.ID,
		quorum:    len(cfg.Peers)/2 + 1,
		term:      st.HardState.Term,
		vote:      st.HardState.Vote,
		next:      map[int]uint64{},
		match:     map[int]uint64{},
		lastAck:   map[int]time.Time{},
		peerRound: map[int]uint64{},
		nudge:     map[int]chan struct{}{},
		hbNudge:   map[int]chan struct{}{},
		flushCh:   make(chan struct{}, 1),
		applyCh:   make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID))),
	}
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.others = append(n.others, p)
			n.nudge[p] = make(chan struct{}, 1)
			n.hbNudge[p] = make(chan struct{}, 1)
		}
	}
	n.log = append([]Entry{{Index: st.Snapshot.Index, Term: st.Snapshot.Term}}, st.Entries...)
	n.snapshot = st.Snapshot
	n.commit, n.applied = st.Snapshot.Index, st.Snapshot.Index
	n.durable = n.lastIndex()
	if st.Snapshot.Index > 0 {
		s := st.Snapshot
		n.pendingSnap = &s
		n.applied = 0 // the applier delivers the snapshot first
	}
	n.resetElectionTimer()

	n.wg.Add(3 + 2*len(n.others))
	go n.ticker()
	go n.applier()
	go n.flusher()
	for _, p := range n.others {
		go n.replicator(p)
		go n.heartbeater(p)
	}
	n.signal(n.applyCh)
	return n, nil
}

// Stop halts all goroutines. The node must not be used afterwards.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	for _, w := range n.readWaiters {
		close(w.ch)
	}
	n.readWaiters = nil
	n.mu.Unlock()
	n.wg.Wait()
}

// Status is a point-in-time view for monitoring and tests.
type Status struct {
	ID, Leader      int
	Role            Role
	Term            uint64
	Commit, Applied uint64
	LastIndex       uint64
	SnapshotIndex   uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, Leader: n.lead, Role: n.role, Term: n.term, Commit: n.commit, Applied: n.applied,
		LastIndex: n.lastIndex(), SnapshotIndex: n.log[0].Index}
}

// ---------------------------------------------------------------------------
// Log helpers (caller holds mu)

func (n *Node) lastIndex() uint64 { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }
func (n *Node) snapIndex() uint64 { return n.log[0].Index }

// termAt returns the term of entry i, or ok=false if i is compacted or beyond the log.
func (n *Node) termAt(i uint64) (uint64, bool) {
	if i < n.snapIndex() || i > n.lastIndex() {
		return 0, false
	}
	return n.log[i-n.snapIndex()].Term, true
}

func (n *Node) entriesFrom(i uint64, max int) []Entry {
	lo := i - n.snapIndex()
	hi := uint64(len(n.log))
	if hi-lo > uint64(max) {
		hi = lo + uint64(max)
	}
	return append([]Entry(nil), n.log[lo:hi]...)
}

func (n *Node) upToDate(lastIdx, lastTerm uint64) bool {
	return lastTerm > n.lastTerm() || (lastTerm == n.lastTerm() && lastIdx >= n.lastIndex())
}

func (n *Node) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (n *Node) persistHardState() {
	if err := n.cfg.Storage.SetHardState(HardState{Term: n.term, Vote: n.vote}); err != nil {
		panic(fmt.Sprintf("raft: persist hard state: %v", err))
	}
}

func (n *Node) syncLocked() {
	if err := n.cfg.Storage.Sync(); err != nil {
		panic(fmt.Sprintf("raft: sync: %v", err))
	}
	n.durable = n.lastIndex()
}

func (n *Node) resetElectionTimer() {
	t := n.cfg.ElectionTimeout
	n.electionDeadline = time.Now().Add(t + time.Duration(n.rng.Int63n(int64(t))))
}

func (n *Node) becomeFollower(term uint64, lead int) {
	if term > n.term {
		n.term = term
		n.vote = 0
		n.persistHardState()
		// The new term must be durable before we answer anyone in it.
		n.syncLocked()
	}
	if n.role != Follower {
		n.cfg.Logger.Info("raft: became follower", "id", n.id, "term", n.term)
	}
	n.role = Follower
	n.lead = lead
	for _, w := range n.readWaiters {
		close(w.ch) // waiters re-check leadership and fail
	}
	n.readWaiters = nil
}

// ---------------------------------------------------------------------------
// Elections

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval / 3)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case now := <-t.C:
			n.mu.Lock()
			switch {
			case n.role == Leader:
				n.checkQuorum(now)
			case now.After(n.electionDeadline):
				n.resetElectionTimer()
				go n.campaign(true)
			}
			n.mu.Unlock()
		}
	}
}

// checkQuorum steps down a leader that cannot reach a majority, so a
// partitioned leader stops accepting work (and stops serving ReadIndex).
func (n *Node) checkQuorum(now time.Time) {
	alive := 1
	for _, p := range n.others {
		if now.Sub(n.lastAck[p]) < n.cfg.ElectionTimeout {
			alive++
		}
	}
	if alive < n.quorum {
		n.cfg.Logger.Info("raft: leader lost quorum, stepping down", "id", n.id, "term", n.term)
		n.becomeFollower(n.term, 0)
		n.resetElectionTimer()
	}
}

// campaign runs a PreVote round and, if it succeeds, a real election. PreVote
// keeps a node that was partitioned away from incrementing its term and
// deposing a healthy leader when it rejoins.
func (n *Node) campaign(pre bool) {
	n.mu.Lock()
	if n.stopped || n.role == Leader || (!pre && n.role != PreCandidate) {
		n.mu.Unlock()
		return
	}
	var term uint64
	if pre {
		n.role = PreCandidate
		term = n.term + 1
	} else {
		n.role = Candidate
		n.term++
		n.vote = n.id
		n.persistHardState()
		n.syncLocked()
		term = n.term
	}
	n.lead = 0
	n.resetElectionTimer()
	myTerm, role := n.term, n.role
	args := &RequestVoteArgs{Term: term, Candidate: n.id, LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(), PreVote: pre}
	n.cfg.Logger.Debug("raft: campaign", "id", n.id, "term", term, "pre", pre)
	if n.quorum == 1 {
		n.winLocked(pre)
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	votes := 1
	for _, p := range n.others {
		go func(p int) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			reply, err := n.cfg.Transport.RequestVote(ctx, p, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if reply.Term > n.term {
				if !pre || !reply.Granted {
					n.becomeFollower(reply.Term, 0)
				}
				return
			}
			if n.term != myTerm || n.role != role || !reply.Granted {
				return
			}
			votes++
			if votes == n.quorum {
				n.winLocked(pre)
			}
		}(p)
	}
}

func (n *Node) winLocked(pre bool) {
	if pre {
		go n.campaign(false)
		return
	}
	n.role = Leader
	n.lead = n.id
	n.cfg.Logger.Info("raft: became leader", "id", n.id, "term", n.term)
	now := time.Now()
	for _, p := range n.others {
		n.next[p] = n.lastIndex() + 1
		n.match[p] = 0
		n.lastAck[p] = now // grace period before CheckQuorum can fire
		n.peerRound[p] = 0
	}
	n.readRound = 0
	// Commit a no-op from this term so earlier entries become committed and
	// ReadIndex has a current-term commit to anchor on.
	n.appendLocked(nil)
}

func (n *Node) HandleRequestVote(a *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &RequestVoteReply{Term: n.term}
	if n.stopped {
		return reply
	}
	// Leader stickiness: ignore candidates while we have a live leader. This
	// pairs with CheckQuorum so a leader that is really gone loses this
	// protection within one election timeout.
	stickyLeader := n.role == Leader ||
		(n.lead != 0 && time.Since(n.leaderContact) < n.cfg.ElectionTimeout)

	if a.PreVote {
		reply.Granted = a.Term > n.term && !stickyLeader && n.upToDate(a.LastLogIndex, a.LastLogTerm)
		return reply
	}
	if a.Term < n.term {
		return reply
	}
	if a.Term > n.term {
		if stickyLeader {
			return reply
		}
		n.becomeFollower(a.Term, 0)
		reply.Term = n.term
	}
	if (n.vote == 0 || n.vote == a.Candidate) && n.upToDate(a.LastLogIndex, a.LastLogTerm) {
		n.vote = a.Candidate
		n.persistHardState()
		n.syncLocked()
		n.resetElectionTimer()
		reply.Granted = true
	}
	return reply
}

// ---------------------------------------------------------------------------
// Proposals and replication

// Propose appends data to the log if this node is the leader. The entry is
// committed once a majority has it durably; watch ApplyMsg for (index, term).
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if data == nil {
		data = []byte{}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, 0, ErrStopped
	}
	if n.role != Leader {
		return 0, n.term, ErrNotLeader
	}
	idx := n.appendLocked(data)
	return idx, n.term, nil
}

func (n *Node) appendLocked(data []byte) uint64 {
	e := Entry{Term: n.term, Index: n.lastIndex() + 1, Data: data}
	n.log = append(n.log, e)
	if err := n.cfg.Storage.Append([]Entry{e}); err != nil {
		panic(fmt.Sprintf("raft: append: %v", err))
	}
	n.signal(n.flushCh)
	for _, p := range n.others {
		n.signal(n.nudge[p])
	}
	if n.quorum == 1 {
		n.advanceCommitLocked()
	}
	return e.Index
}

// flusher group-commits the leader's own log: while one fsync runs, new
// proposals accumulate and are covered by the next one.
func (n *Node) flusher() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.flushCh:
		}
		n.mu.Lock()
		target, gen := n.lastIndex(), n.truncs
		n.mu.Unlock()
		if err := n.cfg.Storage.Sync(); err != nil {
			panic(fmt.Sprintf("raft: sync: %v", err))
		}
		n.mu.Lock()
		if n.truncs == gen && target > n.durable {
			n.durable = target
			if n.role == Leader {
				n.advanceCommitLocked()
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) advanceCommitLocked() {
	matches := []uint64{n.durable}
	for _, p := range n.others {
		matches = append(matches, n.match[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	cand := matches[n.quorum-1]
	if cand > n.lastIndex() {
		cand = n.lastIndex()
	}
	// Only entries from the current term are committed by counting replicas
	// (Raft paper, figure 8).
	if t, ok := n.termAt(cand); ok && cand > n.commit && t == n.term {
		n.commit = cand
		n.signal(n.applyCh)
	}
}

func (n *Node) replicator(p int) {
	defer n.wg.Done()
	hb := time.NewTimer(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.nudge[p]:
		case <-hb.C:
		}
		if !hb.Stop() {
			select {
			case <-hb.C:
			default:
			}
		}
		hb.Reset(n.cfg.HeartbeatInterval)
		for n.replicateOnce(p) {
		}
	}
}

// heartbeater sends empty AppendEntries on its own schedule, independent of
// the replicator's (possibly slow, fsync-bound) in-flight request. Replies
// only refresh liveness and ReadIndex rounds.
func (n *Node) heartbeater(p int) {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		case <-n.hbNudge[p]:
		}
		n.mu.Lock()
		if n.role != Leader || n.stopped {
			n.mu.Unlock()
			continue
		}
		term, round := n.term, n.readRound
		prev := n.match[p]
		prevTerm, ok := n.termAt(prev)
		if !ok {
			prev, prevTerm = n.snapIndex(), n.log[0].Term
		}
		args := &AppendEntriesArgs{Term: term, Leader: n.id, PrevLogIndex: prev, PrevLogTerm: prevTerm, LeaderCommit: n.commit}
		n.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		reply, err := n.cfg.Transport.AppendEntries(ctx, p, args)
		cancel()
		if err != nil {
			continue
		}
		n.mu.Lock()
		n.ackLocked(p, term, reply.Term, round)
		n.mu.Unlock()
	}
}

// replicateOnce sends one AppendEntries or InstallSnapshot to p. It returns
// true if p is still behind and another round should go out immediately.
func (n *Node) replicateOnce(p int) bool {
	n.mu.Lock()
	if n.role != Leader || n.stopped {
		n.mu.Unlock()
		return false
	}
	term, round := n.term, n.readRound
	next := n.next[p]
	if next <= n.snapIndex() {
		args := &InstallSnapshotArgs{Term: term, Leader: n.id, LastIncludedIndex: n.snapshot.Index,
			LastIncludedTerm: n.snapshot.Term, Data: n.snapshot.Data}
		n.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 4*n.cfg.RPCTimeout)
		reply, err := n.cfg.Transport.InstallSnapshot(ctx, p, args)
		cancel()
		if err != nil {
			return false
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if !n.ackLocked(p, term, reply.Term, round) {
			return false
		}
		if args.LastIncludedIndex > n.match[p] {
			n.match[p] = args.LastIncludedIndex
		}
		n.next[p] = n.match[p] + 1
		n.advanceCommitLocked()
		return n.next[p] <= n.lastIndex()
	}
	prevTerm, _ := n.termAt(next - 1)
	args := &AppendEntriesArgs{Term: term, Leader: n.id, PrevLogIndex: next - 1, PrevLogTerm: prevTerm,
		Entries: n.entriesFrom(next, n.cfg.MaxAppendEntries), LeaderCommit: n.commit}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
	reply, err := n.cfg.Transport.AppendEntries(ctx, p, args)
	cancel()
	if err != nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.ackLocked(p, term, reply.Term, round) {
		return false
	}
	if reply.Success {
		if reply.MatchIndex > n.match[p] {
			n.match[p] = reply.MatchIndex
		}
		n.next[p] = n.match[p] + 1
		n.advanceCommitLocked()
	} else {
		next := reply.ConflictIndex
		if reply.ConflictTerm != 0 {
			// Skip past the follower's whole conflicting term if we have it.
			for i := n.lastIndex(); i > n.snapIndex(); i-- {
				if t, _ := n.termAt(i); t == reply.ConflictTerm {
					next = i + 1
					break
				}
			}
		}
		if next < 1 {
			next = 1
		}
		if next > n.lastIndex()+1 {
			next = n.lastIndex() + 1
		}
		if next <= n.match[p] {
			next = n.match[p] + 1
		}
		n.next[p] = next
	}
	return n.next[p] <= n.lastIndex()
}

// ackLocked processes the term of a reply. It returns false if this node is
// no longer leader of the term the request was sent in.
func (n *Node) ackLocked(p int, sentTerm, replyTerm, round uint64) bool {
	if replyTerm > n.term {
		n.becomeFollower(replyTerm, 0)
		n.resetElectionTimer()
		return false
	}
	if n.role != Leader || n.term != sentTerm || n.stopped {
		return false
	}
	n.lastAck[p] = time.Now()
	if round > n.peerRound[p] {
		n.peerRound[p] = round
		n.checkReadsLocked()
	}
	return true
}

func (n *Node) HandleAppendEntries(a *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &AppendEntriesReply{Term: n.term}
	if n.stopped || a.Term < n.term {
		return reply
	}
	if a.Term > n.term || n.role != Follower {
		n.becomeFollower(a.Term, a.Leader)
	}
	n.lead = a.Leader
	n.leaderContact = time.Now()
	n.resetElectionTimer()
	reply.Term = n.term

	prevIdx, prevTerm, entries := a.PrevLogIndex, a.PrevLogTerm, a.Entries
	if prevIdx < n.snapIndex() {
		// The prefix up to our snapshot is committed and therefore matches.
		skip := n.snapIndex() - prevIdx
		if uint64(len(entries)) <= skip {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prevIdx, prevTerm = n.snapIndex(), n.log[0].Term
	}
	if prevIdx > n.lastIndex() {
		reply.ConflictIndex = n.lastIndex() + 1
		return reply
	}
	if t, _ := n.termAt(prevIdx); t != prevTerm {
		reply.ConflictTerm = t
		i := prevIdx
		for i > n.snapIndex()+1 {
			if pt, _ := n.termAt(i - 1); pt != t {
				break
			}
			i--
		}
		reply.ConflictIndex = i
		return reply
	}
	// Append, truncating only on an actual conflict so that a delayed,
	// shorter RPC cannot erase entries a newer RPC already delivered.
	for k, e := range entries {
		if t, ok := n.termAt(e.Index); ok && t == e.Term {
			continue
		}
		if e.Index <= n.lastIndex() {
			if e.Index <= n.commit {
				panic(fmt.Sprintf("raft: node %d asked to truncate committed index %d", n.id, e.Index))
			}
			n.log = n.log[:e.Index-n.snapIndex()]
			n.truncs++
			if n.durable >= e.Index {
				n.durable = e.Index - 1
			}
		}
		n.log = append(n.log, entries[k:]...)
		if err := n.cfg.Storage.Append(entries[k:]); err != nil {
			panic(fmt.Sprintf("raft: append: %v", err))
		}
		break
	}
	match := prevIdx + uint64(len(entries))
	if len(entries) == 0 {
		// Heartbeat: never waits for the disk. Only claim what is durable.
		if match > n.durable {
			match = n.durable
		}
	} else if match > n.durable {
		// fsync without holding mu so heartbeats (and therefore ReadIndex
		// confirmations) are not stuck behind the disk. Within one term the
		// leader never rewrites entries it sent, so if neither the term nor
		// the truncation counter changed, what we synced is what we ack.
		target, gen, term := n.lastIndex(), n.truncs, n.term
		n.mu.Unlock()
		err := n.cfg.Storage.Sync()
		n.mu.Lock()
		if err != nil {
			panic(fmt.Sprintf("raft: sync: %v", err))
		}
		if n.stopped || n.truncs != gen || n.term != term {
			reply.Term = n.term
			reply.ConflictIndex = n.commit + 1
			return reply
		}
		if target > n.durable {
			n.durable = target
		}
	}
	if a.LeaderCommit > n.commit {
		c := a.LeaderCommit
		if c > match {
			c = match
		}
		if c > n.commit {
			n.commit = c
			n.signal(n.applyCh)
		}
	}
	reply.Success = true
	reply.MatchIndex = match
	return reply
}

// ---------------------------------------------------------------------------
// Snapshots

// Snapshot tells Raft that the service has captured its state up to and
// including index; the log prefix is discarded.
func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index <= n.snapIndex() || index > n.applied || index > n.lastIndex() {
		return nil
	}
	t, _ := n.termAt(index)
	tail := append([]Entry(nil), n.log[index-n.snapIndex()+1:]...)
	snap := Snapshot{Index: index, Term: t, Data: data}
	if err := n.cfg.Storage.SaveSnapshot(snap, HardState{Term: n.term, Vote: n.vote}, tail); err != nil {
		return err
	}
	n.log = append([]Entry{{Index: index, Term: t}}, tail...)
	n.snapshot = snap
	return nil
}

func (n *Node) HandleInstallSnapshot(a *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := &InstallSnapshotReply{Term: n.term}
	if n.stopped || a.Term < n.term {
		return reply
	}
	if a.Term > n.term || n.role != Follower {
		n.becomeFollower(a.Term, a.Leader)
	}
	n.lead = a.Leader
	n.leaderContact = time.Now()
	n.resetElectionTimer()
	reply.Term = n.term
	if a.LastIncludedIndex <= n.commit {
		return reply // we already have (and may have applied) this prefix
	}
	var tail []Entry
	if t, ok := n.termAt(a.LastIncludedIndex); ok && t == a.LastIncludedTerm {
		tail = append(tail, n.log[a.LastIncludedIndex-n.snapIndex()+1:]...)
	}
	snap := Snapshot{Index: a.LastIncludedIndex, Term: a.LastIncludedTerm, Data: a.Data}
	if err := n.cfg.Storage.SaveSnapshot(snap, HardState{Term: n.term, Vote: n.vote}, tail); err != nil {
		panic(fmt.Sprintf("raft: save snapshot: %v", err))
	}
	n.log = append([]Entry{{Index: snap.Index, Term: snap.Term}}, tail...)
	n.snapshot = snap
	n.truncs++
	n.durable = n.lastIndex()
	n.commit = snap.Index
	n.pendingSnap = &snap
	n.signal(n.applyCh)
	return reply
}

// ---------------------------------------------------------------------------
// Applying committed entries

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
		}
		for {
			n.mu.Lock()
			var msgs []ApplyMsg
			if s := n.pendingSnap; s != nil {
				n.pendingSnap = nil
				if s.Index > n.applied {
					n.applied = s.Index
					msgs = append(msgs, ApplyMsg{SnapshotValid: true, Snapshot: *s})
				}
			} else if n.applied < n.commit {
				from := n.applied + 1
				if from <= n.snapIndex() {
					from = n.snapIndex() + 1
				}
				for i := from; i <= n.commit; i++ {
					e := n.log[i-n.snapIndex()]
					msgs = append(msgs, ApplyMsg{CommandValid: true, Index: e.Index, Term: e.Term, Data: e.Data})
				}
				n.applied = n.commit
			}
			n.mu.Unlock()
			if len(msgs) == 0 {
				break
			}
			for _, m := range msgs {
				select {
				case n.cfg.Apply <- m:
				case <-n.stopCh:
					return
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// ReadIndex

// ReadIndex returns an index such that a read served from the state machine
// after applying through that index is linearizable. It confirms leadership
// with a heartbeat round acknowledged by a majority, without writing to the
// log (Raft dissertation, section 6.4).
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return 0, ErrStopped
	}
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	// The commit index is only known to be up to date once the leader has
	// committed an entry from its own term.
	if t, _ := n.termAt(n.commit); t != n.term {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	idx := n.commit
	if n.quorum == 1 {
		n.mu.Unlock()
		return idx, nil
	}
	n.readRound++
	w := readWaiter{round: n.readRound, ch: make(chan struct{})}
	n.readWaiters = append(n.readWaiters, w)
	term := n.term
	for _, p := range n.others {
		n.signal(n.hbNudge[p])
	}
	n.mu.Unlock()

	select {
	case <-w.ch:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, ErrStopped
	}
	if n.role != Leader || n.term != term {
		return 0, ErrNotLeader
	}
	return idx, nil
}

func (n *Node) checkReadsLocked() {
	if len(n.readWaiters) == 0 {
		return
	}
	rounds := []uint64{}
	for _, p := range n.others {
		rounds = append(rounds, n.peerRound[p])
	}
	sort.Slice(rounds, func(i, j int) bool { return rounds[i] > rounds[j] })
	confirmed := rounds[n.quorum-2] // quorum-1 peers plus ourselves
	k := 0
	for _, w := range n.readWaiters {
		if w.round <= confirmed {
			close(w.ch)
		} else {
			n.readWaiters[k] = w
			k++
		}
	}
	n.readWaiters = n.readWaiters[:k]
}
