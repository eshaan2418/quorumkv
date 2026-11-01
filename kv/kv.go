// Package kv is a replicated key-value state machine on top of package raft.
//
// Writes go through the Raft log and are deduplicated per client session, so
// a client that retries after a timeout gets exactly-once semantics. Reads
// use ReadIndex by default: no log write, but still linearizable.
package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eshaan2418/quorumkv/raft"
)

type OpKind uint8

const (
	OpGet OpKind = iota
	OpPut
	OpAppend
	OpDelete
)

// Request is a client operation. ClientID/Seq identify a write for
// deduplication; a client must use increasing Seq values and have at most
// one write outstanding.
type Request struct {
	Kind     OpKind
	Key      string
	Value    string
	ClientID uint64
	Seq      uint64
}

type Response struct {
	Value string
	Found bool
}

// ReadMode selects how Gets are served.
type ReadMode int

const (
	// ReadIndex confirms leadership with a heartbeat round, then reads
	// locally once the state machine has caught up (linearizable).
	ReadIndex ReadMode = iota
	// ReadLog sends every Get through the Raft log (linearizable, slower).
	ReadLog
	// ReadLocalUnsafe serves Gets from whatever node receives them, with no
	// coordination. NOT linearizable; exists to demonstrate that the
	// linearizability checker catches stale reads.
	ReadLocalUnsafe
)

func ParseReadMode(s string) (ReadMode, error) {
	switch s {
	case "index", "readindex", "":
		return ReadIndex, nil
	case "log":
		return ReadLog, nil
	case "unsafe-local":
		return ReadLocalUnsafe, nil
	}
	return 0, fmt.Errorf("unknown read mode %q (index, log, unsafe-local)", s)
}

var (
	ErrNotLeader = errors.New("kv: not the leader")
	// ErrUnknown means the request may or may not have been applied; retry
	// with the same (ClientID, Seq) to find out safely.
	ErrUnknown = errors.New("kv: outcome unknown")
)

type Options struct {
	ReadMode      ReadMode
	SnapshotEvery uint64 // entries between snapshots; 0 disables
}

type session struct {
	Seq    uint64
	Result Response
}

type waiter struct {
	term uint64
	ch   chan applied
}

type applied struct {
	term uint64
	resp Response
}

type Store struct {
	opts Options
	node *raft.Node

	mu       sync.Mutex
	data     map[string]string
	sessions map[uint64]session
	index    uint64        // last applied log index
	notify   chan struct{} // closed and replaced on every apply
	waiters  map[uint64]waiter
	lastSnap uint64
	stopped  bool
}

func New(opts Options) *Store {
	return &Store{opts: opts, data: map[string]string{}, sessions: map[uint64]session{},
		notify: make(chan struct{}), waiters: map[uint64]waiter{}}
}

// Run starts applying committed entries from ch (the channel passed to
// raft.New as Config.Apply). It returns when ch is closed.
func (s *Store) Run(node *raft.Node, ch <-chan raft.ApplyMsg) {
	s.mu.Lock()
	s.node = node
	s.mu.Unlock()
	go s.applyLoop(ch)
}

func (s *Store) Node() *raft.Node { return s.node }

func (s *Store) applyLoop(ch <-chan raft.ApplyMsg) {
	for m := range ch {
		s.mu.Lock()
		if m.SnapshotValid {
			s.restoreLocked(m.Snapshot)
		} else if m.Index > s.index {
			var resp Response
			if len(m.Data) > 0 {
				req, err := decodeRequest(m.Data)
				if err != nil {
					panic(fmt.Sprintf("kv: corrupt log entry %d: %v", m.Index, err))
				}
				resp = s.applyLocked(req)
			}
			s.index = m.Index
			if w, ok := s.waiters[m.Index]; ok {
				w.ch <- applied{term: m.Term, resp: resp}
				delete(s.waiters, m.Index)
			}
			if s.opts.SnapshotEvery > 0 && s.index-s.lastSnap >= s.opts.SnapshotEvery {
				s.lastSnap = s.index
				if err := s.node.Snapshot(s.index, s.encodeLocked()); err != nil {
					panic(fmt.Sprintf("kv: snapshot: %v", err))
				}
			}
		}
		close(s.notify)
		s.notify = make(chan struct{})
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.stopped = true
	for idx, w := range s.waiters {
		w.ch <- applied{}
		delete(s.waiters, idx)
	}
	close(s.notify)
	s.mu.Unlock()
}

func (s *Store) applyLocked(r Request) Response {
	if r.Kind == OpGet {
		v, ok := s.data[r.Key]
		return Response{Value: v, Found: ok}
	}
	if r.ClientID != 0 {
		if sess, ok := s.sessions[r.ClientID]; ok && r.Seq <= sess.Seq {
			return sess.Result // duplicate of an already-applied write
		}
	}
	var resp Response
	switch r.Kind {
	case OpPut:
		s.data[r.Key] = r.Value
	case OpAppend:
		s.data[r.Key] += r.Value
	case OpDelete:
		_, resp.Found = s.data[r.Key]
		delete(s.data, r.Key)
	}
	if r.ClientID != 0 {
		s.sessions[r.ClientID] = session{Seq: r.Seq, Result: resp}
	}
	return resp
}

type snapshotState struct {
	Data     map[string]string
	Sessions map[uint64]session
}

func (s *Store) encodeLocked() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotState{Data: s.data, Sessions: s.sessions}); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (s *Store) restoreLocked(snap raft.Snapshot) {
	if snap.Index <= s.index {
		return
	}
	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(snap.Data)).Decode(&st); err != nil {
		panic(fmt.Sprintf("kv: corrupt snapshot: %v", err))
	}
	if st.Data == nil {
		st.Data = map[string]string{}
	}
	if st.Sessions == nil {
		st.Sessions = map[uint64]session{}
	}
	s.data, s.sessions = st.Data, st.Sessions
	s.index, s.lastSnap = snap.Index, snap.Index
	// Entries covered by the snapshot will never be applied individually.
	for idx, w := range s.waiters {
		if idx <= snap.Index {
			w.ch <- applied{}
			delete(s.waiters, idx)
		}
	}
}

// Do executes a request. Writes (and Gets in ReadLog mode) return
// ErrNotLeader if this node cannot accept them and ErrUnknown if the
// outcome is uncertain.
func (s *Store) Do(ctx context.Context, r Request) (Response, error) {
	if r.Kind == OpGet {
		switch s.opts.ReadMode {
		case ReadLocalUnsafe:
			return s.read(r.Key), nil
		case ReadIndex:
			idx, err := s.node.ReadIndex(ctx)
			if err != nil {
				if errors.Is(err, raft.ErrNotLeader) {
					return Response{}, ErrNotLeader
				}
				return Response{}, err
			}
			if err := s.waitApplied(ctx, idx); err != nil {
				return Response{}, err
			}
			return s.read(r.Key), nil
		}
		r.ClientID, r.Seq = 0, 0
	}
	return s.propose(ctx, r)
}

func (s *Store) read(key string) Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return Response{Value: v, Found: ok}
}

func (s *Store) waitApplied(ctx context.Context, idx uint64) error {
	for {
		s.mu.Lock()
		if s.index >= idx {
			s.mu.Unlock()
			return nil
		}
		if s.stopped {
			s.mu.Unlock()
			return raft.ErrStopped
		}
		ch := s.notify
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Store) propose(ctx context.Context, r Request) (Response, error) {
	data := encodeRequest(r)
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return Response{}, raft.ErrStopped
	}
	// Register before the entry can possibly be applied (applyLoop needs mu).
	idx, term, err := s.node.Propose(data)
	if err != nil {
		s.mu.Unlock()
		if errors.Is(err, raft.ErrNotLeader) {
			return Response{}, ErrNotLeader
		}
		return Response{}, err
	}
	ch := make(chan applied, 1)
	s.waiters[idx] = waiter{term: term, ch: ch}
	s.mu.Unlock()

	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case a := <-ch:
			if a.term != term {
				// A different entry was committed at idx: ours was lost, or
				// it was folded into a snapshot. Either way the caller must
				// retry (dedup makes that safe).
				return Response{}, ErrUnknown
			}
			return a.resp, nil
		case <-tick.C:
			if st := s.node.Status(); st.Term != term || st.Role != raft.Leader {
				s.drop(idx, ch)
				return Response{}, ErrUnknown
			}
		case <-ctx.Done():
			s.drop(idx, ch)
			return Response{}, ErrUnknown
		}
	}
}

func (s *Store) drop(idx uint64, ch chan applied) {
	s.mu.Lock()
	if w, ok := s.waiters[idx]; ok && w.ch == ch {
		delete(s.waiters, idx)
	}
	s.mu.Unlock()
}

// Len returns the number of keys (for tests and status).
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// ---------------------------------------------------------------------------
// Wire encoding of log entries: kind u8 | client u64 | seq u64 | key | value,
// strings as u32 length + bytes.

func encodeRequest(r Request) []byte {
	b := make([]byte, 0, 1+16+8+len(r.Key)+len(r.Value))
	b = append(b, byte(r.Kind))
	b = binary.LittleEndian.AppendUint64(b, r.ClientID)
	b = binary.LittleEndian.AppendUint64(b, r.Seq)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Key)))
	b = append(b, r.Key...)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Value)))
	return append(b, r.Value...)
}

func decodeRequest(b []byte) (Request, error) {
	var r Request
	if len(b) < 21 {
		return r, errors.New("short request")
	}
	r.Kind = OpKind(b[0])
	r.ClientID = binary.LittleEndian.Uint64(b[1:])
	r.Seq = binary.LittleEndian.Uint64(b[9:])
	b = b[17:]
	str := func() (string, error) {
		if len(b) < 4 {
			return "", errors.New("short string header")
		}
		n := binary.LittleEndian.Uint32(b)
		if uint32(len(b)-4) < n {
			return "", errors.New("short string")
		}
		s := string(b[4 : 4+n])
		b = b[4+n:]
		return s, nil
	}
	var err error
	if r.Key, err = str(); err != nil {
		return r, err
	}
	if r.Value, err = str(); err != nil {
		return r, err
	}
	return r, nil
}
