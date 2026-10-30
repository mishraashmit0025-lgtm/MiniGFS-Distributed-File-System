// Package chunkserver stores fixed-size chunks on local disk, verifies them
// with CRC32-C checksums, and replicates writes along a chain of servers.
package chunkserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/proto/gfspb"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Checksum is the CRC32-C used for chunk data end to end.
func Checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

type Config struct {
	Address           string   // advertised address
	Dir               string   // chunk storage directory
	Masters           []string // all master addresses (heartbeats go to each)
	HeartbeatInterval time.Duration
	CapacityBytes     uint64 // reported capacity (free = capacity - used)
}

type Server struct {
	pb.UnimplementedChunkServerServer
	cfg  Config
	grpc *grpc.Server

	mu     sync.RWMutex
	chunks map[uint64]uint64 // handle -> size
	used   uint64
	locks  sync.Map // handle -> *sync.Mutex (serialize writes per chunk)

	flightMu sync.Mutex
	flights  map[uint64]*flight // in-progress disk reads, shared by concurrent readers

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
	stop   chan struct{}
	wg     sync.WaitGroup
}

func New(cfg Config) (*Server, error) {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 500 * time.Millisecond
	}
	if cfg.CapacityBytes == 0 {
		cfg.CapacityBytes = 100 << 30
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, chunks: map[uint64]uint64{}, conns: map[string]*grpc.ClientConn{}, stop: make(chan struct{}), flights: map[uint64]*flight{}}
	if err := s.scan(); err != nil {
		return nil, err
	}
	s.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(256<<20), grpc.MaxSendMsgSize(256<<20))
	pb.RegisterChunkServerServer(s.grpc, s)
	return s, nil
}

// scan rebuilds the in-memory chunk table from disk after a restart.
func (s *Server) scan() error {
	ents, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".chunk") {
			continue
		}
		h, err := strconv.ParseUint(strings.TrimSuffix(name, ".chunk"), 10, 64)
		if err != nil {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		size := uint64(info.Size()) - 4 // trailing checksum
		s.chunks[h] = size
		s.used += size
	}
	return nil
}

func (s *Server) Serve(lis net.Listener) error {
	s.wg.Add(1)
	go s.heartbeatLoop()
	return s.grpc.Serve(lis)
}

// Stop simulates a crash: stops serving immediately.
func (s *Server) Stop() {
	select {
	case <-s.stop:
		return
	default:
	}
	close(s.stop)
	s.grpc.Stop()
	s.wg.Wait()
	s.connMu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.connMu.Unlock()
}

func (s *Server) path(h uint64) string { return filepath.Join(s.cfg.Dir, fmt.Sprintf("%d.chunk", h)) }

func (s *Server) lock(h uint64) *sync.Mutex {
	l, _ := s.locks.LoadOrStore(h, &sync.Mutex{})
	return l.(*sync.Mutex)
}

// store writes data+checksum atomically (temp file, fsync, rename).
func (s *Server) store(h uint64, data []byte, sum uint32) error {
	tmp := s.path(h) + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], sum)
	if _, err = f.Write(data); err == nil {
		_, err = f.Write(tail[:])
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, s.path(h)); err != nil {
		return err
	}
	s.mu.Lock()
	if old, ok := s.chunks[h]; ok {
		s.used -= old
	}
	s.chunks[h] = uint64(len(data))
	s.used += uint64(len(data))
	s.mu.Unlock()
	return nil
}

// load reads a chunk and verifies its checksum.
func (s *Server) load(h uint64) ([]byte, error) {
	raw, err := os.ReadFile(s.path(h))
	if errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.NotFound, "chunk %d not on %s", h, s.cfg.Address)
	}
	if err != nil {
		return nil, err
	}
	if len(raw) < 4 {
		return nil, status.Errorf(codes.DataLoss, "chunk %d truncated", h)
	}
	data, want := raw[:len(raw)-4], binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if Checksum(data) != want {
		return nil, status.Errorf(codes.DataLoss, "chunk %d checksum mismatch on %s", h, s.cfg.Address)
	}
	return data, nil
}

func (s *Server) peer(addr string) (pb.ChunkServerClient, error) {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	c, ok := s.conns[addr]
	if !ok {
		var err error
		c, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20), grpc.MaxCallSendMsgSize(256<<20)))
		if err != nil {
			return nil, err
		}
		s.conns[addr] = c
	}
	return pb.NewChunkServerClient(c), nil
}

// WriteChunk stores the data locally, then forwards it down the replica chain.
// It returns only after every downstream replica has stored the data.
func (s *Server) WriteChunk(ctx context.Context, r *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	if Checksum(r.Data) != r.Checksum {
		return nil, status.Error(codes.DataLoss, "checksum mismatch in transit")
	}
	l := s.lock(r.Handle)
	l.Lock()
	err := s.store(r.Handle, r.Data, r.Checksum)
	l.Unlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "store chunk %d: %v", r.Handle, err)
	}
	if len(r.ForwardTo) > 0 {
		next, err := s.peer(r.ForwardTo[0])
		if err != nil {
			return nil, err
		}
		if _, err := next.WriteChunk(ctx, &pb.WriteChunkRequest{
			Handle: r.Handle, Data: r.Data, Checksum: r.Checksum, ForwardTo: r.ForwardTo[1:],
		}); err != nil {
			return nil, status.Errorf(codes.Unavailable, "forward to %s: %v", r.ForwardTo[0], err)
		}
	}
	return &pb.WriteChunkResponse{}, nil
}

type flight struct {
	done chan struct{}
	data []byte
	err  error
}

// loadShared coalesces concurrent reads of the same chunk into a single disk read and
// checksum verification. Nothing is cached once the read completes, so every new read
// still re-verifies the bytes on disk.
func (s *Server) loadShared(h uint64) ([]byte, error) {
	s.flightMu.Lock()
	if f, ok := s.flights[h]; ok {
		s.flightMu.Unlock()
		<-f.done
		return f.data, f.err
	}
	f := &flight{done: make(chan struct{})}
	s.flights[h] = f
	s.flightMu.Unlock()

	f.data, f.err = s.load(h)
	s.flightMu.Lock()
	delete(s.flights, h)
	s.flightMu.Unlock()
	close(f.done)
	return f.data, f.err
}

func (s *Server) ReadChunk(_ context.Context, r *pb.ReadChunkRequest) (*pb.ReadChunkResponse, error) {
	data, err := s.loadShared(r.Handle)
	if err != nil {
		return nil, err
	}
	if r.Offset > uint64(len(data)) {
		return nil, status.Error(codes.OutOfRange, "offset past end of chunk")
	}
	end := uint64(len(data))
	if r.Length > 0 && r.Offset+r.Length < end {
		end = r.Offset + r.Length
	}
	return &pb.ReadChunkResponse{Data: data[r.Offset:end]}, nil
}

// CopyChunk pushes a verified local chunk to another server (used for re-replication).
func (s *Server) CopyChunk(ctx context.Context, r *pb.CopyChunkRequest) (*pb.CopyChunkResponse, error) {
	data, err := s.load(r.Handle)
	if err != nil {
		return nil, err
	}
	c, err := s.peer(r.Target)
	if err != nil {
		return nil, err
	}
	_, err = c.WriteChunk(ctx, &pb.WriteChunkRequest{Handle: r.Handle, Data: data, Checksum: Checksum(data)})
	return &pb.CopyChunkResponse{}, err
}

// Handles lists stored chunk handles.
func (s *Server) Handles() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint64, 0, len(s.chunks))
	for h := range s.chunks {
		out = append(out, h)
	}
	return out
}

func (s *Server) remove(h uint64) {
	l := s.lock(h)
	l.Lock()
	defer l.Unlock()
	s.mu.Lock()
	if size, ok := s.chunks[h]; ok {
		s.used -= size
		delete(s.chunks, h)
	}
	s.mu.Unlock()
	_ = os.Remove(s.path(h))
}

func (s *Server) heartbeatLoop() {
	defer s.wg.Done()
	masters := make([]pb.MasterClient, 0, len(s.cfg.Masters))
	for _, m := range s.cfg.Masters {
		c, err := grpc.NewClient(m, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("chunkserver %s: dial master %s: %v", s.cfg.Address, m, err)
			continue
		}
		defer c.Close()
		masters = append(masters, pb.NewMasterClient(c))
	}
	beat := func() {
		s.mu.RLock()
		free := uint64(0)
		if s.cfg.CapacityBytes > s.used {
			free = s.cfg.CapacityBytes - s.used
		}
		s.mu.RUnlock()
		req := &pb.HeartbeatRequest{Address: s.cfg.Address, Handles: s.Handles(), FreeBytes: free}
		var wg sync.WaitGroup
		for _, m := range masters {
			wg.Add(1)
			go func(m pb.MasterClient) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HeartbeatInterval)
				defer cancel()
				resp, err := m.Heartbeat(ctx, req)
				if err != nil {
					return
				}
				for _, h := range resp.Garbage {
					s.remove(h)
				}
			}(m)
		}
		wg.Wait()
	}
	beat()
	t := time.NewTicker(s.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			beat()
		}
	}
}
