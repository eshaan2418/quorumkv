package lin

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestSequentialHistory(t *testing.T) {
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Get, Key: "x", Output: "1", Found: true, Call: 2, Return: 3},
		{Kind: Append, Key: "x", Value: "2", Call: 4, Return: 5},
		{Kind: Get, Key: "x", Output: "12", Found: true, Call: 6, Return: 7},
		{Kind: Delete, Key: "x", Call: 8, Return: 9},
		{Kind: Get, Key: "x", Found: false, Call: 10, Return: 11},
	}
	if r := Check(h, time.Second); !r.Ok {
		t.Fatal(r)
	}
}

func TestStaleReadIsRejected(t *testing.T) {
	// The read starts after the second write completed but sees the first.
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Put, Key: "x", Value: "2", Call: 2, Return: 3},
		{Kind: Get, Key: "x", Output: "1", Found: true, Call: 4, Return: 5},
	}
	if r := Check(h, time.Second); r.Ok || r.Key != "x" {
		t.Fatal("stale read accepted:", r)
	}
}

func TestConcurrentReadMaySeeEither(t *testing.T) {
	for _, out := range []string{"1", "2"} {
		h := []Op{
			{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
			{Kind: Put, Key: "x", Value: "2", Call: 2, Return: 10},
			{Kind: Get, Key: "x", Output: out, Found: true, Call: 3, Return: 4},
		}
		if r := Check(h, time.Second); !r.Ok {
			t.Fatalf("output %s: %v", out, r)
		}
	}
}

func TestPendingWriteMayOrMayNotApply(t *testing.T) {
	base := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Put, Key: "x", Value: "2", Call: 2, Return: Pending},
	}
	for _, out := range []string{"1", "2"} {
		h := append(append([]Op{}, base...), Op{Kind: Get, Key: "x", Output: out, Found: true, Call: 5, Return: 6})
		if r := Check(h, time.Second); !r.Ok {
			t.Fatalf("output %s: %v", out, r)
		}
	}
	// ...but once observed, it cannot be un-applied.
	h := append(append([]Op{}, base...),
		Op{Kind: Get, Key: "x", Output: "2", Found: true, Call: 5, Return: 6},
		Op{Kind: Get, Key: "x", Output: "1", Found: true, Call: 7, Return: 8})
	if r := Check(h, time.Second); r.Ok {
		t.Fatal("write un-applied after being observed")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Op{
		{Kind: Put, Key: "a", Value: "1", Call: 0, Return: 1},
		{Kind: Put, Key: "b", Value: "2", Call: 0, Return: 1},
		{Kind: Get, Key: "a", Output: "1", Found: true, Call: 2, Return: 3},
		{Kind: Get, Key: "b", Output: "1", Found: true, Call: 2, Return: 3},
	}
	if r := Check(h, time.Second); r.Ok || r.Key != "b" {
		t.Fatal(r)
	}
}

// ---------------------------------------------------------------------------
// Cross-check against Porcupine on random single-key histories.

// genHistory simulates clients against an atomic register: each op takes
// effect at a random instant inside [call, return], so the history is
// linearizable by construction. corrupt perturbs one read afterwards.
func genHistory(rng *rand.Rand, clients, perClient int, corrupt bool) []Op {
	type pending struct {
		op *Op
		at int64
	}
	var ops []Op
	for c := 0; c < clients; c++ {
		t := int64(rng.Intn(5))
		for i := 0; i < perClient; i++ {
			call := t + int64(rng.Intn(3))
			ret := call + 1 + int64(rng.Intn(12))
			op := Op{Client: c, Key: "k", Call: call, Return: ret}
			switch rng.Intn(4) {
			case 0, 1:
				op.Kind = Get
			case 2:
				op.Kind = Put
				op.Value = fmt.Sprint(rng.Intn(4))
			default:
				op.Kind = Append
				op.Value = fmt.Sprint(rng.Intn(4))
			}
			ops = append(ops, op)
			t = ret + 1
		}
	}
	var ps []pending
	for i := range ops {
		ps = append(ps, pending{&ops[i], ops[i].Call + rng.Int63n(ops[i].Return-ops[i].Call+1)})
	}
	sort.SliceStable(ps, func(i, j int) bool { return ps[i].at < ps[j].at })
	s := state{}
	for _, p := range ps {
		if p.op.Kind == Get {
			p.op.Output, p.op.Found = s.value, s.exists
		}
		s, _ = step(s, p.op)
	}
	if corrupt {
		for i := range ops {
			if ops[i].Kind == Get && rng.Intn(3) == 0 {
				ops[i].Output += fmt.Sprint(rng.Intn(3))
				ops[i].Found = true
			}
		}
	}
	// Some writes never return.
	for i := range ops {
		if ops[i].Kind != Get && rng.Intn(10) == 0 {
			ops[i].Return = Pending
		}
	}
	return ops
}

var porcupineModel = porcupine.Model{
	Init: func() interface{} { return state{} },
	Step: func(s, in, out interface{}) (bool, interface{}) {
		op := in.(Op)
		if op.Kind == Get {
			o := out.(Op)
			op.Output, op.Found = o.Output, o.Found
		}
		ns, ok := step(s.(state), &op)
		return ok, ns
	},
	Equal: func(a, b interface{}) bool { return a.(state) == b.(state) },
}

func TestAgreesWithPorcupine(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	agree, nonlin := 0, 0
	for trial := 0; trial < 2000; trial++ {
		h := genHistory(rng, 1+rng.Intn(5), 1+rng.Intn(8), trial%2 == 1)
		var ph []porcupine.Operation
		for _, op := range h {
			ph = append(ph, porcupine.Operation{ClientId: op.Client, Input: op, Output: op, Call: op.Call, Return: op.Return})
		}
		want := porcupine.CheckOperations(porcupineModel, ph)
		got, timeout := CheckKey(h, 10*time.Second)
		if timeout {
			t.Fatal("timeout on small history")
		}
		if got != want {
			t.Fatalf("trial %d: lin=%v porcupine=%v\n%+v", trial, got, want, h)
		}
		if trial%2 == 0 && !got {
			t.Fatalf("trial %d: generated-linearizable history rejected\n%+v", trial, h)
		}
		agree++
		if !got {
			nonlin++
		}
	}
	if nonlin < 200 {
		t.Fatalf("only %d non-linearizable histories generated; test is too weak", nonlin)
	}
	t.Logf("%d histories agree with porcupine (%d non-linearizable)", agree, nonlin)
}

func TestLargeHistoryIsFast(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	var h []Op
	for k := 0; k < 20; k++ {
		for _, op := range genHistory(rng, 8, 60, false) {
			op.Key = fmt.Sprint("key", k)
			h = append(h, op)
		}
	}
	start := time.Now()
	r := Check(h, 30*time.Second)
	if !r.Ok {
		t.Fatal(r)
	}
	t.Logf("%s in %v", r, time.Since(start))
}
