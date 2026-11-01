package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Storage persists Raft state. Writes become durable only after Sync, except
// SaveSnapshot which is durable on return. Append with entries[0].Index <=
// the last stored index truncates the conflicting suffix first.
type Storage interface {
	Load() (State, error)
	SetHardState(hs HardState) error
	Append(entries []Entry) error
	Sync() error
	// SaveSnapshot durably replaces the log prefix up to snap.Index with
	// snap, keeping `tail` (entries after snap.Index) and hs.
	SaveSnapshot(snap Snapshot, hs HardState, tail []Entry) error
	Close() error
}

// State is what Load recovers.
type State struct {
	HardState HardState
	Snapshot  Snapshot // Index == 0 if none
	Entries   []Entry  // entries after Snapshot.Index, contiguous
}

// ---------------------------------------------------------------------------
// MemStorage: for tests. Everything written is treated as durable, so a node
// "restarted" with the same MemStorage sees all of its acknowledged state.

type MemStorage struct {
	mu    sync.Mutex
	state State
}

func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) Load() (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.state
	s.Entries = append([]Entry(nil), m.state.Entries...)
	return s, nil
}

func (m *MemStorage) SetHardState(hs HardState) error {
	m.mu.Lock()
	m.state.HardState = hs
	m.mu.Unlock()
	return nil
}

func (m *MemStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Entries = truncateAppend(m.state.Entries, entries)
	return nil
}

func (m *MemStorage) Sync() error { return nil }

func (m *MemStorage) SaveSnapshot(snap Snapshot, hs HardState, tail []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = State{HardState: hs, Snapshot: snap, Entries: append([]Entry(nil), tail...)}
	return nil
}

func (m *MemStorage) Close() error { return nil }

func truncateAppend(log []Entry, entries []Entry) []Entry {
	first := entries[0].Index
	cut := len(log)
	for cut > 0 && log[cut-1].Index >= first {
		cut--
	}
	return append(log[:cut], entries...)
}

// ---------------------------------------------------------------------------
// FileStorage: a write-ahead log plus a snapshot file.
//
// wal:  sequence of records [u32 len][u32 crc32c(type+payload)][u8 type][payload]
//         type 1 = hard state  (term u64, vote i64)
//         type 2 = entries     (count u32, then per entry: term u64, index u64,
//                               len u32, data; len 0xFFFFFFFF = nil no-op)
// snap: [u32 crc32c][index u64][term u64][data...]
//
// Recovery replays the WAL; a torn or corrupt record ends the log (it can
// only be an un-synced tail) and the file is truncated there. Snapshots are
// written to a temp file, fsynced and renamed; the WAL is then rewritten the
// same way, so a crash at any point leaves a consistent pair.

const (
	recHardState = 1
	recEntries   = 2
	nilData      = 0xFFFFFFFF
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type FileStorage struct {
	mu     sync.Mutex
	dir    string
	wal    *os.File
	w      *bufio.Writer
	noSync bool
	buf    []byte
}

// OpenFileStorage opens (creating if needed) storage in dir. With noSync,
// Sync only flushes to the OS and skips fsync (benchmarking only: unsafe).
func OpenFileStorage(dir string, noSync bool) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir, wal: f, w: bufio.NewWriterSize(f, 1<<20), noSync: noSync}, nil
}

func (s *FileStorage) Load() (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st State
	snap, err := readSnapshot(filepath.Join(s.dir, "snap"))
	if err != nil {
		return st, err
	}
	st.Snapshot = snap

	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return st, err
	}
	r := bufio.NewReaderSize(s.wal, 1<<20)
	var good int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			break
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if n == 0 || n > 1<<30 {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			break
		}
		if crc32.Checksum(body, crcTable) != sum {
			break
		}
		if err := applyRecord(&st, body); err != nil {
			break
		}
		good += 8 + int64(n)
	}
	// Drop anything after the last intact record and position for appends.
	if err := s.wal.Truncate(good); err != nil {
		return st, err
	}
	if _, err := s.wal.Seek(good, io.SeekStart); err != nil {
		return st, err
	}
	s.w.Reset(s.wal)

	// Entries covered by the snapshot are redundant (crash between snapshot
	// rename and WAL rewrite).
	k := 0
	for k < len(st.Entries) && st.Entries[k].Index <= st.Snapshot.Index {
		k++
	}
	st.Entries = st.Entries[k:]
	if len(st.Entries) > 0 && st.Entries[0].Index != st.Snapshot.Index+1 {
		return st, fmt.Errorf("raft: wal gap: snapshot at %d, first entry %d", st.Snapshot.Index, st.Entries[0].Index)
	}
	return st, nil
}

func applyRecord(st *State, body []byte) error {
	typ, p := body[0], body[1:]
	switch typ {
	case recHardState:
		if len(p) != 16 {
			return errors.New("bad hard state")
		}
		st.HardState = HardState{Term: binary.LittleEndian.Uint64(p), Vote: int(int64(binary.LittleEndian.Uint64(p[8:])))}
	case recEntries:
		entries, err := decodeEntries(p)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			st.Entries = truncateAppend(st.Entries, entries)
		}
	default:
		return errors.New("unknown record")
	}
	return nil
}

func decodeEntries(p []byte) ([]Entry, error) {
	if len(p) < 4 {
		return nil, errors.New("short entries")
	}
	count := binary.LittleEndian.Uint32(p)
	p = p[4:]
	out := make([]Entry, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(p) < 20 {
			return nil, errors.New("short entry")
		}
		e := Entry{Term: binary.LittleEndian.Uint64(p), Index: binary.LittleEndian.Uint64(p[8:])}
		n := binary.LittleEndian.Uint32(p[16:])
		p = p[20:]
		if n != nilData {
			if uint32(len(p)) < n {
				return nil, errors.New("short entry data")
			}
			e.Data = append([]byte{}, p[:n]...)
			p = p[n:]
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *FileStorage) record(typ byte, payload func([]byte) []byte) error {
	s.buf = append(s.buf[:0], 0, 0, 0, 0, 0, 0, 0, 0, typ)
	s.buf = payload(s.buf)
	body := s.buf[8:]
	binary.LittleEndian.PutUint32(s.buf[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(s.buf[4:8], crc32.Checksum(body, crcTable))
	_, err := s.w.Write(s.buf)
	return err
}

func appendHardState(b []byte, hs HardState) []byte {
	b = binary.LittleEndian.AppendUint64(b, hs.Term)
	return binary.LittleEndian.AppendUint64(b, uint64(int64(hs.Vote)))
}

func appendEntries(b []byte, entries []Entry) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(entries)))
	for _, e := range entries {
		b = binary.LittleEndian.AppendUint64(b, e.Term)
		b = binary.LittleEndian.AppendUint64(b, e.Index)
		if e.Data == nil {
			b = binary.LittleEndian.AppendUint32(b, nilData)
			continue
		}
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	return b
}

func (s *FileStorage) SetHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record(recHardState, func(b []byte) []byte { return appendHardState(b, hs) })
}

func (s *FileStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record(recEntries, func(b []byte) []byte { return appendEntries(b, entries) })
}

// Sync flushes buffered records and fsyncs the WAL. Concurrent callers
// coalesce naturally: whoever holds the lock flushes everything buffered.
func (s *FileStorage) Sync() error {
	s.mu.Lock()
	if err := s.w.Flush(); err != nil {
		s.mu.Unlock()
		return err
	}
	f := s.wal
	s.mu.Unlock()
	if s.noSync {
		return nil
	}
	// On darwin, (*os.File).Sync issues F_FULLFSYNC.
	return f.Sync()
}

func (s *FileStorage) SaveSnapshot(snap Snapshot, hs HardState, tail []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := writeSnapshot(filepath.Join(s.dir, "snap"), snap, !s.noSync); err != nil {
		return err
	}
	// Rewrite the WAL with only the state after the snapshot.
	tmp := filepath.Join(s.dir, "wal.tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	s.w.Reset(f)
	if err := s.record(recHardState, func(b []byte) []byte { return appendHardState(b, hs) }); err != nil {
		f.Close()
		return err
	}
	if len(tail) > 0 {
		if err := s.record(recEntries, func(b []byte) []byte { return appendEntries(b, tail) }); err != nil {
			f.Close()
			return err
		}
	}
	if err := s.w.Flush(); err != nil {
		f.Close()
		return err
	}
	if !s.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "wal")); err != nil {
		f.Close()
		return err
	}
	if !s.noSync {
		if err := syncDir(s.dir); err != nil {
			f.Close()
			return err
		}
	}
	s.wal.Close()
	s.wal = f
	return nil
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.wal.Close()
}

func writeSnapshot(path string, snap Snapshot, sync bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	body := binary.LittleEndian.AppendUint64(nil, snap.Index)
	body = binary.LittleEndian.AppendUint64(body, snap.Term)
	body = append(body, snap.Data...)
	hdr := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, crcTable))
	if _, err := f.Write(append(hdr, body...)); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if sync {
		return syncDir(filepath.Dir(path))
	}
	return nil
}

func readSnapshot(path string) (Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	if len(b) < 20 || crc32.Checksum(b[4:], crcTable) != binary.LittleEndian.Uint32(b) {
		// Snapshots are renamed into place only after fsync, so this is real
		// corruption, not a torn write.
		return Snapshot{}, fmt.Errorf("raft: corrupt snapshot %s", path)
	}
	return Snapshot{
		Index: binary.LittleEndian.Uint64(b[4:]),
		Term:  binary.LittleEndian.Uint64(b[12:]),
		Data:  b[20:],
	}, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
