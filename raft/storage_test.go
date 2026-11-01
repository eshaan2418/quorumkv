package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func entries(from, to, term uint64) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Term: term, Index: i, Data: []byte(fmt.Sprintf("e%d-t%d", i, term))})
	}
	return out
}

func reopen(t *testing.T, dir string) (*FileStorage, State) {
	t.Helper()
	s, err := OpenFileStorage(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	return s, st
}

func TestFileStorageRoundTripAndTruncation(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	must(t, s.SetHardState(HardState{Term: 2, Vote: 3}))
	must(t, s.Append(entries(1, 10, 1)))
	must(t, s.Append([]Entry{{Term: 2, Index: 4, Data: nil}})) // truncates 4..10, nil = no-op
	must(t, s.Append(entries(5, 6, 2)))
	must(t, s.Sync())
	must(t, s.Close())

	s, st := reopen(t, dir)
	defer s.Close()
	if st.HardState != (HardState{Term: 2, Vote: 3}) {
		t.Fatalf("hard state %+v", st.HardState)
	}
	want := append(entries(1, 3, 1), Entry{Term: 2, Index: 4})
	want = append(want, entries(5, 6, 2)...)
	if !reflect.DeepEqual(st.Entries, want) {
		t.Fatalf("entries\n got %+v\nwant %+v", st.Entries, want)
	}
}

func TestFileStorageTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	must(t, s.Append(entries(1, 5, 1)))
	must(t, s.Sync())
	must(t, s.Append(entries(6, 8, 1)))
	must(t, s.Sync())
	must(t, s.Close())

	// Chop the last record in half, as a crash mid-write would.
	path := filepath.Join(dir, "wal")
	fi, _ := os.Stat(path)
	must(t, os.Truncate(path, fi.Size()-7))

	s, st := reopen(t, dir)
	if got := len(st.Entries); got != 5 {
		t.Fatalf("recovered %d entries, want 5", got)
	}
	// The torn bytes are gone, so new appends land after the good prefix.
	must(t, s.Append(entries(6, 6, 2)))
	must(t, s.Sync())
	must(t, s.Close())
	_, st = reopen(t, dir)
	if len(st.Entries) != 6 || st.Entries[5].Term != 2 {
		t.Fatalf("append after torn-tail recovery: %+v", st.Entries)
	}
}

func TestFileStorageCorruptRecordEndsLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	for i := uint64(1); i <= 4; i++ {
		must(t, s.Append(entries(i, i, 1)))
	}
	must(t, s.Sync())
	must(t, s.Close())
	path := filepath.Join(dir, "wal")
	b, _ := os.ReadFile(path)
	b[len(b)-3] ^= 0xFF // flip a byte in the last record's payload
	must(t, os.WriteFile(path, b, 0o644))
	_, st := reopen(t, dir)
	if len(st.Entries) != 3 {
		t.Fatalf("got %d entries after corrupt tail, want 3", len(st.Entries))
	}
}

func TestFileStorageSnapshotCompacts(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	must(t, s.Append(entries(1, 100, 1)))
	must(t, s.Sync())
	before, _ := os.Stat(filepath.Join(dir, "wal"))
	snap := Snapshot{Index: 90, Term: 1, Data: []byte("state@90")}
	must(t, s.SaveSnapshot(snap, HardState{Term: 1, Vote: 1}, entries(91, 100, 1)))
	must(t, s.Append(entries(101, 102, 1)))
	must(t, s.Sync())
	must(t, s.Close())
	after, _ := os.Stat(filepath.Join(dir, "wal"))
	if after.Size() >= before.Size() {
		t.Fatalf("wal did not shrink: %d -> %d", before.Size(), after.Size())
	}
	_, st := reopen(t, dir)
	if !reflect.DeepEqual(st.Snapshot, snap) {
		t.Fatalf("snapshot %+v", st.Snapshot)
	}
	if len(st.Entries) != 12 || st.Entries[0].Index != 91 || st.Entries[11].Index != 102 {
		t.Fatalf("entries after snapshot: first=%d n=%d", st.Entries[0].Index, len(st.Entries))
	}
}

// A crash after the snapshot is renamed into place but before the WAL is
// rewritten leaves an old WAL that still contains the compacted prefix.
func TestFileStorageCrashBetweenSnapshotAndWALRewrite(t *testing.T) {
	dir := t.TempDir()
	s, _ := reopen(t, dir)
	must(t, s.Append(entries(1, 20, 1)))
	must(t, s.Sync())
	must(t, s.Close())
	must(t, writeSnapshot(filepath.Join(dir, "snap"), Snapshot{Index: 15, Term: 1, Data: []byte("x")}, true))
	_, st := reopen(t, dir)
	if st.Snapshot.Index != 15 || len(st.Entries) != 5 || st.Entries[0].Index != 16 {
		t.Fatalf("snapshot=%d entries=%d first=%d", st.Snapshot.Index, len(st.Entries), st.Entries[0].Index)
	}
}

func TestFileStorageCorruptSnapshotIsAnError(t *testing.T) {
	dir := t.TempDir()
	must(t, writeSnapshot(filepath.Join(dir, "snap"), Snapshot{Index: 3, Term: 1, Data: []byte("abc")}, true))
	b, _ := os.ReadFile(filepath.Join(dir, "snap"))
	b[len(b)-1] ^= 1
	must(t, os.WriteFile(filepath.Join(dir, "snap"), b, 0o644))
	s, err := OpenFileStorage(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("corrupt snapshot loaded without error")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// BenchmarkFileStorageAppendSync measures one durable WAL append: a 128-byte
// entry followed by fsync (F_FULLFSYNC on macOS). This is the floor for
// write latency on each replica.
func BenchmarkFileStorageAppendSync(b *testing.B) {
	s, err := OpenFileStorage(b.TempDir(), false)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	data := make([]byte, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Append([]Entry{{Term: 1, Index: uint64(i + 1), Data: data}}); err != nil {
			b.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
