// Package raft is a compact implementation of the Raft consensus algorithm
// (Ongaro & Ousterhout, 2014): randomized leader election, log replication with
// fast conflict back-off, commitment by majority in the current term, and
// in-order application to a state machine. Term, vote and log are persisted to
// disk (when a data directory is configured) before any RPC reply that depends on them.
package raft

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	pb "github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/proto/gfspb"
)

// ErrNotLeader is returned by Propose on followers; Leader holds the best-known leader id.
type ErrNotLeader struct{ Leader string }

func (e ErrNotLeader) Error() string { return fmt.Sprintf("not leader (leader=%q)", e.Leader) }

// ErrStopped is returned after Stop.
var ErrStopped = errors.New("raft: node stopped")

// Transport sends Raft RPCs to a peer, identified by its id.
type Transport interface {
	RequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

// Config configures a node. Peers excludes the node itself.
type Config struct {
	ID                 string
	Peers              []string
	Transport          Transport
	Apply              func(command []byte) any // state machine; called in log order, one at a time
	DataDir            string                   // "" = volatile
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
}

func (c *Config) defaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 20 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 60 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.HeartbeatInterval * 3
	}
}

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string { return [...]string{"follower", "candidate", "leader"}[r] }

type persistent struct {
	Term     uint64
	VotedFor string
	Log      []entry // Log[0] is a sentinel with index 0, term 0
}

type entry struct {
	Term    uint64
	Command []byte
}

type waiter struct {
	term uint64
	ch   chan any
}

// Node is one Raft participant.
type Node struct {
	cfg Config

	mu          sync.Mutex
	role        role
	term        uint64
	votedFor    string
	log         []entry
	commitIndex uint64
	lastApplied uint64
	leaderID    string
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	deadline    time.Time
	waiters     map[uint64]waiter
	stopped     bool

	applyCond *sync.Cond
	kick      map[string]chan struct{} // wake a peer's replicator
	stopCh    chan struct{}
	wg        sync.WaitGroup
	rng       *rand.Rand
}

// New creates a node and restores persisted state, if any.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	n := &Node{
		cfg:     cfg,
		log:     []entry{{}},
		waiters: map[uint64]waiter{},
		kick:    map[string]chan struct{}{},
		stopCh:  make(chan struct{}),
		rng:     rand.New(rand.NewSource(seed())),
	}
	n.applyCond = sync.NewCond(&n.mu)
	for _, p := range cfg.Peers {
		n.kick[p] = make(chan struct{}, 1)
	}
	if err := n.restore(); err != nil {
		return nil, err
	}
	return n, nil
}

// seed returns a cryptographically random seed. Clock-based seeds collide on hosts with a
// coarse timer, giving every node the same "random" election timeout and endless split votes.
func seed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// Start launches the election timer, replicators and applier.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetDeadline()
	n.mu.Unlock()
	n.wg.Add(2 + len(n.cfg.Peers))
	go n.ticker()
	go n.applier()
	for _, p := range n.cfg.Peers {
		go n.replicator(p)
	}
}

// Stop halts the node; pending proposals fail with ErrStopped.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	for idx, w := range n.waiters {
		w.ch <- ErrStopped
		delete(n.waiters, idx)
	}
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// State returns the current term, whether this node is leader, and the known leader id.
func (n *Node) State() (term uint64, isLeader bool, leaderID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term, n.role == leader, n.leaderID
}

// CommitIndex returns the highest committed log index.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// Propose appends a command and blocks until it is committed and applied,
// returning the state machine's result.
func (n *Node) Propose(ctx context.Context, command []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != leader {
		l := n.leaderID
		n.mu.Unlock()
		return nil, ErrNotLeader{Leader: l}
	}
	n.log = append(n.log, entry{Term: n.term, Command: command})
	idx := n.lastIndex()
	if err := n.persist(); err != nil {
		n.log = n.log[:idx]
		n.mu.Unlock()
		return nil, err
	}
	w := waiter{term: n.term, ch: make(chan any, 1)}
	n.waiters[idx] = w
	n.matchIndex[n.cfg.ID] = idx
	n.advanceCommit() // single-node cluster commits immediately
	n.mu.Unlock()
	n.kickAll()

	select {
	case res := <-w.ch:
		if err, ok := res.(error); ok {
			return nil, err
		}
		return res, nil
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, idx)
		n.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ---------------------------------------------------------------- RPC handlers

// HandleRequestVote implements the RequestVote RPC receiver.
func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	grant := false
	if req.Term == n.term && (n.votedFor == "" || n.votedFor == req.CandidateId) {
		lastTerm := n.log[n.lastIndex()].Term
		upToDate := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= n.lastIndex())
		if upToDate {
			grant = true
			n.votedFor = req.CandidateId
			n.resetDeadline()
		}
	}
	if err := n.persist(); err != nil {
		return nil, err
	}
	return &pb.RequestVoteResponse{Term: n.term, VoteGranted: grant}, nil
}

// HandleAppendEntries implements the AppendEntries RPC receiver.
func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.term {
		return &pb.AppendEntriesResponse{Term: n.term}, nil
	}
	if req.Term > n.term || n.role != follower {
		n.becomeFollower(req.Term, req.LeaderId)
	}
	n.leaderID = req.LeaderId
	n.resetDeadline()

	if req.PrevLogIndex > n.lastIndex() {
		return &pb.AppendEntriesResponse{Term: n.term, ConflictIndex: n.lastIndex() + 1}, nil
	}
	if t := n.log[req.PrevLogIndex].Term; t != req.PrevLogTerm {
		ci := req.PrevLogIndex
		for ci > 1 && n.log[ci-1].Term == t {
			ci--
		}
		return &pb.AppendEntriesResponse{Term: n.term, ConflictIndex: ci}, nil
	}
	changed := false
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint64(i)
		if idx <= n.lastIndex() {
			if n.log[idx].Term == e.Term {
				continue
			}
			n.log = n.log[:idx] // conflict: drop this and everything after
		}
		n.log = append(n.log, entry{Term: e.Term, Command: e.Command})
		changed = true
	}
	if changed {
		if err := n.persist(); err != nil {
			return nil, err
		}
	}
	if req.LeaderCommit > n.commitIndex {
		last := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = min(req.LeaderCommit, last)
		n.applyCond.Broadcast()
	}
	return &pb.AppendEntriesResponse{Term: n.term, Success: true}, nil
}

// ---------------------------------------------------------------- internals (mu held unless noted)

func (n *Node) lastIndex() uint64 { return uint64(len(n.log) - 1) }

func (n *Node) resetDeadline() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.deadline = time.Now().Add(n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)+1)))
}

func (n *Node) becomeFollower(term uint64, leaderID string) {
	if term > n.term {
		n.term, n.votedFor = term, ""
	}
	n.role = follower
	n.leaderID = leaderID
	// fail proposals from an older leadership; their entries may be overwritten
	for idx, w := range n.waiters {
		if w.term < n.term {
			w.ch <- ErrNotLeader{Leader: leaderID}
			delete(n.waiters, idx)
		}
	}
}

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval / 2)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		switch {
		case n.role == leader:
			n.mu.Unlock()
			n.kickAll() // heartbeats
		case time.Now().After(n.deadline):
			n.startElection()
			n.mu.Unlock()
		default:
			n.mu.Unlock()
		}
	}
}

func (n *Node) startElection() {
	n.role = candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	n.resetDeadline()
	if err := n.persist(); err != nil {
		return
	}
	term := n.term
	req := &pb.RequestVoteRequest{
		Term: term, CandidateId: n.cfg.ID, LastLogIndex: n.lastIndex(), LastLogTerm: n.log[n.lastIndex()].Term,
	}
	votes := 1
	if votes > (len(n.cfg.Peers)+1)/2 {
		n.becomeLeader()
		return
	}
	for _, p := range n.cfg.Peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.becomeFollower(resp.Term, "")
				_ = n.persist()
				return
			}
			if n.role != candidate || n.term != term || !resp.VoteGranted {
				return
			}
			votes++
			if votes > (len(n.cfg.Peers)+1)/2 {
				n.becomeLeader()
			}
		}(p)
	}
}

func (n *Node) becomeLeader() {
	n.role = leader
	n.leaderID = n.cfg.ID
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{n.cfg.ID: n.lastIndex()}
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	// commit a no-op so entries from earlier terms become committable (Raft §5.4.2)
	n.log = append(n.log, entry{Term: n.term})
	n.matchIndex[n.cfg.ID] = n.lastIndex()
	_ = n.persist()
	n.advanceCommit()
	go n.kickAll()
}

func (n *Node) kickAll() {
	for _, ch := range n.kick {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// replicator sends AppendEntries to one peer whenever kicked (new entries or heartbeat).
func (n *Node) replicator(peer string) {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.kick[peer]:
		}
		n.mu.Lock()
		if n.role != leader {
			n.mu.Unlock()
			continue
		}
		next := n.nextIndex[peer]
		if next < 1 {
			next = 1
		}
		prev := next - 1
		entries := make([]*pb.LogEntry, 0, n.lastIndex()-prev)
		for i := next; i <= n.lastIndex() && len(entries) < 512; i++ {
			entries = append(entries, &pb.LogEntry{Term: n.log[i].Term, Index: i, Command: n.log[i].Command})
		}
		req := &pb.AppendEntriesRequest{
			Term: n.term, LeaderId: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: n.log[prev].Term,
			Entries: entries, LeaderCommit: n.commitIndex,
		}
		term := n.term
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		resp, err := n.cfg.Transport.AppendEntries(ctx, peer, req)
		cancel()
		if err != nil {
			continue
		}
		n.mu.Lock()
		if resp.Term > n.term {
			n.becomeFollower(resp.Term, "")
			_ = n.persist()
			n.mu.Unlock()
			continue
		}
		if n.role != leader || n.term != term {
			n.mu.Unlock()
			continue
		}
		more := false
		if resp.Success {
			m := prev + uint64(len(entries))
			if m > n.matchIndex[peer] {
				n.matchIndex[peer] = m
			}
			n.nextIndex[peer] = m + 1
			n.advanceCommit()
			more = n.nextIndex[peer] <= n.lastIndex()
		} else {
			n.nextIndex[peer] = max(1, min(resp.ConflictIndex, n.lastIndex()+1))
			more = true
		}
		n.mu.Unlock()
		if more {
			select {
			case n.kick[peer] <- struct{}{}:
			default:
			}
		}
	}
}

// advanceCommit moves commitIndex to the highest index replicated on a majority in the current term.
func (n *Node) advanceCommit() {
	matches := make([]uint64, 0, len(n.matchIndex))
	for _, m := range n.matchIndex {
		matches = append(matches, m)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	quorum := matches[(len(n.cfg.Peers)+1)/2]
	if quorum > n.commitIndex && n.log[quorum].Term == n.term {
		n.commitIndex = quorum
		n.applyCond.Broadcast()
	}
}

func (n *Node) applier() {
	defer n.wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}
		idx := n.lastApplied + 1
		e := n.log[idx]
		n.mu.Unlock()
		var res any
		if len(e.Command) > 0 && n.cfg.Apply != nil {
			res = n.cfg.Apply(e.Command)
		}
		n.mu.Lock()
		n.lastApplied = idx
		if w, ok := n.waiters[idx]; ok {
			if w.term == e.Term {
				w.ch <- res
			} else {
				w.ch <- ErrNotLeader{Leader: n.leaderID}
			}
			delete(n.waiters, idx)
		}
	}
}

// ---------------------------------------------------------------- persistence

func (n *Node) statePath() string {
	return filepath.Join(n.cfg.DataDir, "raft-"+sanitize(n.cfg.ID)+".gob")
}

func sanitize(s string) string {
	out := []rune(s)
	for i, r := range out {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
			out[i] = '_'
		}
	}
	return string(out)
}

// persist writes term, vote and log atomically (write temp file, fsync, rename).
func (n *Node) persist() error {
	if n.cfg.DataDir == "" {
		return nil
	}
	if err := os.MkdirAll(n.cfg.DataDir, 0o755); err != nil {
		return err
	}
	tmp := n.statePath() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := gob.NewEncoder(f).Encode(persistent{Term: n.term, VotedFor: n.votedFor, Log: n.log}); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, n.statePath())
}

func (n *Node) restore() error {
	if n.cfg.DataDir == "" {
		return nil
	}
	f, err := os.Open(n.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	var p persistent
	if err := gob.NewDecoder(f).Decode(&p); err != nil {
		return fmt.Errorf("raft: corrupt state file: %w", err)
	}
	n.term, n.votedFor = p.Term, p.VotedFor
	if len(p.Log) > 0 {
		n.log = p.Log
	}
	return nil
}
