package master

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/raft"
	pb "github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/proto/gfspb"
)

// NotLeaderPrefix starts the error message returned by followers; the rest is the leader address.
const NotLeaderPrefix = "not leader; leader="

type Config struct {
	Address          string   // this master's advertised address (also its Raft id)
	Peers            []string // other masters
	DataDir          string   // Raft state; "" = in-memory
	ChunkSize        uint64
	Replication      int
	HeartbeatTimeout time.Duration // chunkserver considered dead after this
	RereplicateEvery time.Duration
	RaftHeartbeat    time.Duration
	RaftElectionMin  time.Duration
	RaftElectionMax  time.Duration
}

func (c *Config) defaults() {
	if c.ChunkSize == 0 {
		c.ChunkSize = 64 << 20
	}
	if c.Replication == 0 {
		c.Replication = 3
	}
	if c.HeartbeatTimeout == 0 {
		c.HeartbeatTimeout = 2 * time.Second
	}
	if c.RereplicateEvery == 0 {
		c.RereplicateEvery = time.Second
	}
}

type serverInfo struct {
	lastSeen time.Time
	handles  map[uint64]bool
	free     uint64
}

// Master is one metadata replica.
type Master struct {
	pb.UnimplementedMasterServer
	cfg   Config
	state *State
	raft  *raft.Node
	trans *raft.GRPCTransport
	grpc  *grpc.Server

	mu      sync.Mutex
	servers map[string]*serverInfo // soft state from heartbeats
	conns   map[string]*grpc.ClientConn
	stop    chan struct{}
	wg      sync.WaitGroup
}

// New creates a master; call Serve with a listener bound to cfg.Address.
func New(cfg Config) (*Master, error) {
	cfg.defaults()
	m := &Master{
		cfg: cfg, state: NewState(), trans: raft.NewGRPCTransport(),
		servers: map[string]*serverInfo{}, conns: map[string]*grpc.ClientConn{}, stop: make(chan struct{}),
	}
	node, err := raft.New(raft.Config{
		ID: cfg.Address, Peers: cfg.Peers, Transport: m.trans, Apply: m.state.Apply, DataDir: cfg.DataDir,
		HeartbeatInterval: cfg.RaftHeartbeat, ElectionTimeoutMin: cfg.RaftElectionMin, ElectionTimeoutMax: cfg.RaftElectionMax,
	})
	if err != nil {
		return nil, err
	}
	m.raft = node
	m.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(16 << 20))
	pb.RegisterMasterServer(m.grpc, m)
	pb.RegisterRaftServer(m.grpc, raft.Server{Node: node})
	return m, nil
}

// Serve starts Raft and background work, then blocks serving gRPC on lis.
func (m *Master) Serve(lis net.Listener) error {
	m.raft.Start()
	m.wg.Add(1)
	go m.rereplicator()
	return m.grpc.Serve(lis)
}

// Stop shuts the master down (simulates a crash for tests: no graceful handoff).
func (m *Master) Stop() {
	select {
	case <-m.stop:
		return
	default:
	}
	close(m.stop)
	m.grpc.Stop()
	m.raft.Stop()
	m.wg.Wait()
	m.trans.Close()
	m.mu.Lock()
	for _, c := range m.conns {
		c.Close()
	}
	m.mu.Unlock()
}

// IsLeader reports whether this master currently leads.
func (m *Master) IsLeader() bool {
	_, l, _ := m.raft.State()
	return l
}

func (m *Master) notLeader() error {
	_, _, leader := m.raft.State()
	return status.Error(codes.FailedPrecondition, NotLeaderPrefix+leader)
}

func (m *Master) propose(ctx context.Context, c Command) (Result, error) {
	raw, _ := json.Marshal(c)
	res, err := m.raft.Propose(ctx, raw)
	var nl raft.ErrNotLeader
	if errors.As(err, &nl) {
		return Result{}, status.Error(codes.FailedPrecondition, NotLeaderPrefix+nl.Leader)
	}
	if err != nil {
		return Result{}, status.Error(codes.Unavailable, err.Error())
	}
	r := res.(Result)
	if r.Err != "" {
		code := codes.InvalidArgument
		if strings.HasPrefix(r.Err, "no such file") {
			code = codes.NotFound
		} else if strings.HasPrefix(r.Err, "file exists") {
			code = codes.AlreadyExists
		}
		return r, status.Error(code, r.Err)
	}
	return r, nil
}

// ---------------------------------------------------------------- client-facing RPCs (leader only)

func (m *Master) CreateFile(ctx context.Context, r *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	if !validPath(r.Path) {
		return nil, status.Error(codes.InvalidArgument, "path must start with / and be at most 1024 bytes")
	}
	_, err := m.propose(ctx, Command{Op: "create", Path: r.Path})
	return &pb.CreateFileResponse{}, err
}

func (m *Master) DeleteFile(ctx context.Context, r *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	_, err := m.propose(ctx, Command{Op: "delete", Path: r.Path})
	return &pb.DeleteFileResponse{}, err
}

func (m *Master) ListFiles(_ context.Context, r *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	if !m.IsLeader() {
		return nil, m.notLeader()
	}
	out := &pb.ListFilesResponse{}
	for _, f := range m.state.list(r.Prefix) {
		out.Files = append(out.Files, &pb.FileInfo{Path: f.Path, Size: f.Size, Chunks: uint32(f.Chunks)})
	}
	return out, nil
}

func (m *Master) GetFile(_ context.Context, r *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	if !m.IsLeader() {
		return nil, m.notLeader()
	}
	f, chunks, ok := m.state.file(r.Path)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no such file: %s", r.Path)
	}
	out := &pb.GetFileResponse{Size: f.Size, ChunkSize: m.cfg.ChunkSize}
	for i, c := range chunks {
		if !c.Done {
			return nil, status.Errorf(codes.Unavailable, "file %s is still being written", r.Path)
		}
		out.Chunks = append(out.Chunks, &pb.ChunkLocation{
			Handle: f.Chunks[i], Version: c.Version, Size: c.Size, Replicas: m.locations(f.Chunks[i], c.Replicas),
		})
	}
	return out, nil
}

func (m *Master) AllocateChunk(ctx context.Context, r *pb.AllocateChunkRequest) (*pb.AllocateChunkResponse, error) {
	if !m.IsLeader() {
		return nil, m.notLeader()
	}
	replicas := m.pickServers(m.cfg.Replication, nil)
	if len(replicas) == 0 {
		return nil, status.Error(codes.Unavailable, "no live chunkservers")
	}
	res, err := m.propose(ctx, Command{Op: "alloc", Path: r.Path, Replicas: replicas})
	if err != nil {
		return nil, err
	}
	return &pb.AllocateChunkResponse{Chunk: &pb.ChunkLocation{Handle: res.Handle, Version: 1, Replicas: replicas}}, nil
}

func (m *Master) CommitChunk(ctx context.Context, r *pb.CommitChunkRequest) (*pb.CommitChunkResponse, error) {
	_, err := m.propose(ctx, Command{Op: "commit", Path: r.Path, Handle: r.Handle, Size: r.Size})
	return &pb.CommitChunkResponse{}, err
}

// Heartbeat is accepted by every master (not just the leader) so a newly elected
// leader already knows chunk locations and can serve reads immediately.
func (m *Master) Heartbeat(_ context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	hs := make(map[uint64]bool, len(r.Handles))
	var garbage []uint64
	for _, h := range r.Handles {
		hs[h] = true
		if m.IsLeader() && !m.state.hasChunk(h) {
			garbage = append(garbage, h)
		}
	}
	m.mu.Lock()
	m.servers[r.Address] = &serverInfo{lastSeen: time.Now(), handles: hs, free: r.FreeBytes}
	m.mu.Unlock()
	return &pb.HeartbeatResponse{Garbage: garbage}, nil
}

func (m *Master) Status(context.Context, *pb.StatusRequest) (*pb.StatusResponse, error) {
	term, leader, leaderID := m.raft.State()
	return &pb.StatusResponse{
		Id: m.cfg.Address, Leader: leader, LeaderAddress: leaderID, Term: term,
		CommitIndex: m.raft.CommitIndex(), LiveChunkservers: m.liveServers(),
	}, nil
}

// ---------------------------------------------------------------- placement & liveness

func validPath(p string) bool { return strings.HasPrefix(p, "/") && len(p) <= 1024 }

func (m *Master) liveServers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for addr, s := range m.servers {
		if time.Since(s.lastSeen) < m.cfg.HeartbeatTimeout {
			out = append(out, addr)
		}
	}
	sort.Strings(out)
	return out
}

// pickServers chooses up to n live servers not in exclude, preferring the most free space.
func (m *Master) pickServers(n int, exclude []string) []string {
	m.mu.Lock()
	type cand struct {
		addr string
		free uint64
		tie  int
	}
	var cs []cand
	for addr, s := range m.servers {
		if time.Since(s.lastSeen) < m.cfg.HeartbeatTimeout && !contains(exclude, addr) {
			cs = append(cs, cand{addr, s.free, rand.Int()})
		}
	}
	m.mu.Unlock()
	sort.Slice(cs, func(i, j int) bool {
		// bucket free space to 1 GiB so equal-ish servers are chosen randomly (spreads load)
		fi, fj := cs[i].free>>30, cs[j].free>>30
		if fi != fj {
			return fi > fj
		}
		return cs[i].tie < cs[j].tie
	})
	var out []string
	for i := 0; i < len(cs) && i < n; i++ {
		out = append(out, cs[i].addr)
	}
	return out
}

// locations returns live servers for handle: live assigned replicas first (a fresh write
// may not have been reported by a heartbeat yet), then any other live server reporting it.
func (m *Master) locations(handle uint64, assigned []string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	live := func(a string) (*serverInfo, bool) {
		s, ok := m.servers[a]
		return s, ok && time.Since(s.lastSeen) < m.cfg.HeartbeatTimeout
	}
	for _, a := range assigned {
		if _, ok := live(a); ok {
			out = append(out, a)
		}
	}
	for a := range m.servers {
		if s, ok := live(a); ok && s.handles[handle] && !contains(out, a) {
			out = append(out, a)
		}
	}
	return out
}

func (m *Master) chunkClient(addr string) (pb.ChunkServerClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.conns[addr]
	if !ok {
		var err error
		c, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20), grpc.MaxCallSendMsgSize(256<<20)))
		if err != nil {
			return nil, err
		}
		m.conns[addr] = c
	}
	return pb.NewChunkServerClient(c), nil
}

// rereplicator restores the replication factor of chunks whose replicas died.
func (m *Master) rereplicator() {
	defer m.wg.Done()
	t := time.NewTicker(m.cfg.RereplicateEvery)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
		}
		if !m.IsLeader() {
			continue
		}
		for h, assigned := range m.state.committedChunks() {
			locs := m.locations(h, assigned)
			if len(locs) == 0 || len(locs) >= m.cfg.Replication {
				continue
			}
			targets := m.pickServers(m.cfg.Replication-len(locs), locs)
			for _, target := range targets {
				c, err := m.chunkClient(locs[0])
				if err != nil {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err = c.CopyChunk(ctx, &pb.CopyChunkRequest{Handle: h, Target: target})
				cancel()
				if err != nil {
					log.Printf("master %s: re-replicate chunk %d to %s: %v", m.cfg.Address, h, target, err)
					continue
				}
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				_, _ = m.propose(ctx, Command{Op: "addreplica", Handle: h, Replicas: []string{target}})
				cancel()
				// count it immediately so the next pass does not copy again before the heartbeat arrives
				m.mu.Lock()
				if s, ok := m.servers[target]; ok {
					s.handles[h] = true
				}
				m.mu.Unlock()
			}
		}
	}
}
