package raft

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/proto/gfspb"
)

// GRPCTransport sends Raft RPCs over gRPC; peer ids are "host:port" addresses.
type GRPCTransport struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport() *GRPCTransport { return &GRPCTransport{conns: map[string]*grpc.ClientConn{}} }

func (t *GRPCTransport) client(peer string) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.conns[peer]
	if !ok {
		var err error
		c, err = grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		t.conns[peer] = c
	}
	return pb.NewRaftClient(c), nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Close()
	}
	t.conns = map[string]*grpc.ClientConn{}
}

// Server adapts a Node to the generated gRPC Raft service.
type Server struct {
	pb.UnimplementedRaftServer
	Node *Node
}

func (s Server) RequestVote(_ context.Context, r *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.Node.HandleRequestVote(r)
}

func (s Server) AppendEntries(_ context.Context, r *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.Node.HandleAppendEntries(r)
}

// ---------------------------------------------------------------- in-memory transport for tests

// MemNetwork connects nodes in-process and can partition them.
type MemNetwork struct {
	mu    sync.Mutex
	nodes map[string]*Node
	down  map[string]bool // isolated nodes
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{nodes: map[string]*Node{}, down: map[string]bool{}}
}

func (m *MemNetwork) Register(id string, n *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[id] = n
}

// SetDown isolates (true) or reconnects (false) a node.
func (m *MemNetwork) SetDown(id string, down bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.down[id] = down
}

var errUnreachable = errors.New("unreachable")

func (m *MemNetwork) target(from, to string) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.down[from] || m.down[to] || m.nodes[to] == nil {
		return nil, errUnreachable
	}
	return m.nodes[to], nil
}

// Transport returns the view of the network from node `from`.
func (m *MemNetwork) Transport(from string) Transport { return memTransport{m, from} }

type memTransport struct {
	net  *MemNetwork
	from string
}

func (t memTransport) RequestVote(_ context.Context, peer string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n, err := t.net.target(t.from, peer)
	if err != nil {
		return nil, err
	}
	return n.HandleRequestVote(req)
}

func (t memTransport) AppendEntries(_ context.Context, peer string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n, err := t.net.target(t.from, peer)
	if err != nil {
		return nil, err
	}
	return n.HandleAppendEntries(req)
}
