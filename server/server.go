// Package server exposes a kv.Store over HTTP: Raft RPCs between peers (gob
// bodies on /raft/*), a JSON client API on /kv, /status, and, when chaos mode
// is enabled, /admin/block for injecting network partitions between real
// processes.
package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/eshaan2418/quorumkv/kv"
	"github.com/eshaan2418/quorumkv/raft"
)

// Config describes one server process.
type Config struct {
	ID              int
	Peers           map[int]string // id -> host:port (including self)
	DataDir         string
	NoSync          bool
	SnapshotEvery   uint64
	ReadMode        kv.ReadMode
	ElectionTimeout time.Duration
	Chaos           bool
	Logger          *slog.Logger
}

type Server struct {
	cfg     Config
	store   *kv.Store
	node    *raft.Node
	storage *raft.FileStorage
	trans   *httpTransport
	http    *http.Server
	ln      net.Listener
}

func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: id %d not in peer list", cfg.ID)
	}
	storage, err := raft.OpenFileStorage(cfg.DataDir, cfg.NoSync)
	if err != nil {
		return nil, err
	}
	trans := newHTTPTransport(cfg.ID, cfg.Peers)
	var ids []int
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	ch := make(chan raft.ApplyMsg, 1024)
	store := kv.New(kv.Options{ReadMode: cfg.ReadMode, SnapshotEvery: cfg.SnapshotEvery})
	node, err := raft.New(raft.Config{ID: cfg.ID, Peers: ids, Transport: trans, Storage: storage, Apply: ch,
		ElectionTimeout: cfg.ElectionTimeout, Logger: cfg.Logger})
	if err != nil {
		storage.Close()
		return nil, err
	}
	store.Run(node, ch)
	s := &Server{cfg: cfg, store: store, node: node, storage: storage, trans: trans}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/vote", rpcHandler(s, func(a *raft.RequestVoteArgs) any { return node.HandleRequestVote(a) }))
	mux.HandleFunc("POST /raft/append", rpcHandler(s, func(a *raft.AppendEntriesArgs) any { return node.HandleAppendEntries(a) }))
	mux.HandleFunc("POST /raft/snapshot", rpcHandler(s, func(a *raft.InstallSnapshotArgs) any { return node.HandleInstallSnapshot(a) }))
	mux.HandleFunc("POST /kv", s.handleKV)
	mux.HandleFunc("GET /status", s.handleStatus)
	if cfg.Chaos {
		mux.HandleFunc("POST /admin/block", s.handleBlock)
	}
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

// Serve listens on the configured address and blocks until Close.
func (s *Server) Serve() error {
	ln, err := net.Listen("tcp", s.cfg.Peers[s.cfg.ID])
	if err != nil {
		return err
	}
	s.ln = ln
	err = s.http.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.http.Shutdown(ctx)
	s.node.Stop()
	return s.storage.Close()
}

func rpcHandler[A any](s *Server, call func(*A) any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		from, _ := strconv.Atoi(r.Header.Get("X-From"))
		if s.trans.isBlocked(from) {
			http.Error(w, "blocked", http.StatusServiceUnavailable)
			return
		}
		var args A
		if err := gob.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply := call(&args)
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(reply); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(buf.Bytes())
	}
}

// KVRequest / KVResponse are the JSON client protocol.
type KVRequest struct {
	Op       string `json:"op"` // get, put, append, delete
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ClientID uint64 `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

type KVResponse struct {
	OK     bool   `json:"ok"`
	Value  string `json:"value,omitempty"`
	Found  bool   `json:"found"`
	Error  string `json:"error,omitempty"` // not_leader, unknown, bad_request
	Leader string `json:"leader,omitempty"`
}

var opKinds = map[string]kv.OpKind{"get": kv.OpGet, "put": kv.OpPut, "append": kv.OpAppend, "delete": kv.OpDelete}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	var req KVRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, KVResponse{Error: "bad_request"})
		return
	}
	kind, ok := opKinds[req.Op]
	if !ok {
		writeJSON(w, http.StatusBadRequest, KVResponse{Error: "bad_request"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	resp, err := s.store.Do(ctx, kv.Request{Kind: kind, Key: req.Key, Value: req.Value, ClientID: req.ClientID, Seq: req.Seq})
	out := KVResponse{OK: err == nil, Value: resp.Value, Found: resp.Found}
	switch {
	case err == nil:
	case errors.Is(err, kv.ErrNotLeader):
		out.Error = "not_leader"
		if l := s.node.Status().Leader; l != 0 {
			out.Leader = s.cfg.Peers[l]
		}
	default:
		out.Error = "unknown"
	}
	writeJSON(w, http.StatusOK, out)
}

type StatusResponse struct {
	ID        int    `json:"id"`
	Role      string `json:"role"`
	Term      uint64 `json:"term"`
	Leader    int    `json:"leader"`
	Commit    uint64 `json:"commit"`
	Applied   uint64 `json:"applied"`
	LastIndex uint64 `json:"last_index"`
	Snapshot  uint64 `json:"snapshot_index"`
	Keys      int    `json:"keys"`
	Blocked   []int  `json:"blocked,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := s.node.Status()
	writeJSON(w, http.StatusOK, StatusResponse{ID: st.ID, Role: st.Role.String(), Term: st.Term, Leader: st.Leader,
		Commit: st.Commit, Applied: st.Applied, LastIndex: st.LastIndex, Snapshot: st.SnapshotIndex,
		Keys: s.store.Len(), Blocked: s.trans.blockedList()})
}

// FetchStatus queries GET /status on addr.
func FetchStatus(addr string) (StatusResponse, error) {
	var st StatusResponse
	c := http.Client{Timeout: time.Second}
	resp, err := c.Get("http://" + addr + "/status")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

func (s *Server) handleBlock(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Peers []int `json:"peers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.trans.setBlocked(body.Peers)
	s.cfg.Logger.Info("chaos: blocked peers", "id", s.cfg.ID, "peers", body.Peers)
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// HTTP transport for Raft RPCs.

type httpTransport struct {
	id     int
	peers  map[int]string
	client *http.Client

	mu      sync.RWMutex
	blocked map[int]bool
}

func newHTTPTransport(id int, peers map[int]string) *httpTransport {
	return &httpTransport{id: id, peers: peers, blocked: map[int]bool{}, client: &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 16, IdleConnTimeout: 30 * time.Second,
			DialContext: (&net.Dialer{Timeout: time.Second}).DialContext},
	}}
}

func (t *httpTransport) isBlocked(id int) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.blocked[id]
}

func (t *httpTransport) setBlocked(ids []int) {
	t.mu.Lock()
	t.blocked = map[int]bool{}
	for _, id := range ids {
		t.blocked[id] = true
	}
	t.mu.Unlock()
}

func (t *httpTransport) blockedList() []int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []int
	for id := range t.blocked {
		out = append(out, id)
	}
	return out
}

var errBlocked = errors.New("server: peer blocked (chaos)")

func rpc[R any](ctx context.Context, t *httpTransport, to int, path string, args any) (*R, error) {
	if t.isBlocked(to) {
		return nil, errBlocked
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+t.peers[to]+path, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-From", strconv.Itoa(t.id))
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("server: %s returned %d", path, resp.StatusCode)
	}
	var reply R
	if err := gob.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *httpTransport) RequestVote(ctx context.Context, to int, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return rpc[raft.RequestVoteReply](ctx, t, to, "/raft/vote", a)
}

func (t *httpTransport) AppendEntries(ctx context.Context, to int, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return rpc[raft.AppendEntriesReply](ctx, t, to, "/raft/append", a)
}

func (t *httpTransport) InstallSnapshot(ctx context.Context, to int, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return rpc[raft.InstallSnapshotReply](ctx, t, to, "/raft/snapshot", a)
}
