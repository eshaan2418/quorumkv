// Package lin checks histories of key-value operations for linearizability.
//
// It implements the Wing & Gong search with Lowe's memoization (the same
// algorithm as Porcupine), split per key: a KV history is linearizable iff
// each key's sub-history is (P-compositionality), which keeps the search
// tractable for histories with thousands of operations.
package lin

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type Kind uint8

const (
	Get Kind = iota
	Put
	Append
	Delete
)

func (k Kind) String() string { return [...]string{"get", "put", "append", "delete"}[k] }

// Op is one completed (or timed-out) client operation.
//
// Call and Return are timestamps (any monotonic unit). A write whose outcome
// is unknown (the client gave up) must have Return = math.MaxInt64: it may
// have taken effect at any point after Call, or not at all. Gets with
// unknown outcome carry no information and should be omitted.
type Op struct {
	Client int
	Kind   Kind
	Key    string
	Value  string // Put/Append argument
	Output string // Get result
	Found  bool   // Get: key existed
	Call   int64
	Return int64
}

const Pending = math.MaxInt64

type state struct {
	value  string
	exists bool
}

func step(s state, op *Op) (state, bool) {
	switch op.Kind {
	case Get:
		return s, op.Found == s.exists && op.Output == s.value
	case Put:
		return state{op.Value, true}, true
	case Append:
		return state{s.value + op.Value, true}, true
	case Delete:
		return state{}, true
	}
	return s, false
}

// Result of a check.
type Result struct {
	Ok      bool
	Timeout bool   // search exceeded the time budget; Ok is meaningless
	Key     string // first key found non-linearizable
	Ops     int
	Keys    int
}

func (r Result) String() string {
	switch {
	case r.Timeout:
		return fmt.Sprintf("UNKNOWN (timeout) after checking %d ops on %d keys", r.Ops, r.Keys)
	case r.Ok:
		return fmt.Sprintf("linearizable: %d ops on %d keys", r.Ops, r.Keys)
	default:
		return fmt.Sprintf("NOT linearizable: key %q (%d ops on %d keys)", r.Key, r.Ops, r.Keys)
	}
}

// Check reports whether history is linearizable, giving up after budget.
func Check(history []Op, budget time.Duration) Result {
	byKey := map[string][]*Op{}
	for i := range history {
		op := &history[i]
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	deadline := time.Now().Add(budget)
	res := Result{Ok: true, Ops: len(history), Keys: len(keys)}
	for _, k := range keys {
		ok, timeout := checkKey(byKey[k], deadline)
		if timeout {
			res.Timeout, res.Ok = true, false
			return res
		}
		if !ok {
			res.Ok, res.Key = false, k
			return res
		}
	}
	return res
}

// CheckKey checks a single-key history (exported for tests).
func CheckKey(ops []Op, budget time.Duration) (ok, timeout bool) {
	ptrs := make([]*Op, len(ops))
	for i := range ops {
		ptrs[i] = &ops[i]
	}
	return checkKey(ptrs, time.Now().Add(budget))
}

// ---------------------------------------------------------------------------
// Wing & Gong / Lowe over a doubly linked list of call and return events.

type node struct {
	op         *Op
	id         int
	isCall     bool
	match      *node // call -> its return
	prev, next *node
}

type bitset []uint64

func (b bitset) set(i int)   { b[i/64] |= 1 << (i % 64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << (i % 64) }
func (b bitset) clone() bitset {
	c := make(bitset, len(b))
	copy(c, b)
	return c
}
func (b bitset) equal(o bitset) bool {
	for i := range b {
		if b[i] != o[i] {
			return false
		}
	}
	return true
}
func (b bitset) hash() uint64 {
	h := uint64(14695981039346656037)
	for _, w := range b {
		h ^= w
		h *= 1099511628211
	}
	return h
}

type cacheEntry struct {
	lin bitset
	st  state
}

type frame struct {
	n  *node
	st state
}

func checkKey(ops []*Op, deadline time.Time) (ok, timeout bool) {
	type event struct {
		t      int64
		isCall bool
		id     int
	}
	events := make([]event, 0, 2*len(ops))
	for i, op := range ops {
		events = append(events, event{op.Call, true, i}, event{op.Return, false, i})
	}
	// At equal timestamps, calls sort before returns so the two operations
	// are treated as concurrent (the permissive, conservative choice).
	sort.Slice(events, func(i, j int) bool {
		if events[i].t != events[j].t {
			return events[i].t < events[j].t
		}
		return events[i].isCall && !events[j].isCall
	})
	head := &node{}
	calls := make([]*node, len(ops))
	prev := head
	for _, e := range events {
		nd := &node{op: ops[e.id], id: e.id, isCall: e.isCall, prev: prev}
		prev.next = nd
		prev = nd
		if e.isCall {
			calls[e.id] = nd
		} else {
			calls[e.id].match = nd
		}
	}

	lift := func(c *node) {
		c.prev.next = c.next
		c.next.prev = c.prev
		r := c.match
		r.prev.next = r.next
		if r.next != nil {
			r.next.prev = r.prev
		}
	}
	unlift := func(c *node) {
		r := c.match
		r.prev.next = r
		if r.next != nil {
			r.next.prev = r
		}
		c.prev.next = c
		c.next.prev = c
	}

	lin := make(bitset, (len(ops)+63)/64)
	cache := map[uint64][]cacheEntry{}
	seen := func(b bitset, s state) bool {
		for _, e := range cache[b.hash()] {
			if e.st == s && e.lin.equal(b) {
				return true
			}
		}
		return false
	}
	var stack []frame
	st := state{}
	entry := head.next
	steps := 0
	for head.next != nil {
		if steps++; steps&0xFFF == 0 && time.Now().After(deadline) {
			return false, true
		}
		if entry.isCall {
			if next, ok := step(st, entry.op); ok {
				nl := lin.clone()
				nl.set(entry.id)
				if !seen(nl, next) {
					h := nl.hash()
					cache[h] = append(cache[h], cacheEntry{nl, next})
					stack = append(stack, frame{entry, st})
					st = next
					lin.set(entry.id)
					lift(entry)
					entry = head.next
					continue
				}
			}
			entry = entry.next
			continue
		}
		// Reached the return of an operation that has not been linearized:
		// undo the most recent choice and try the next candidate.
		if len(stack) == 0 {
			return false, false
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		st = top.st
		lin.clear(top.n.id)
		unlift(top.n)
		entry = top.n.next
	}
	return true, false
}
