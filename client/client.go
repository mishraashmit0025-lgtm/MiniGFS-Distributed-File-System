// Package client is the MiniGFS client library: it finds the master leader,
// splits files into chunks, writes them through the replica chain, and reads
// them back from any healthy replica.
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/chunkserver"
	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/master"
	pb "github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/proto/gfspb"
)

type Client struct {
	masters   []string
	chunkSize uint64
	retryFor  time.Duration

	mu     sync.Mutex
	leader string
	conns  map[string]*grpc.ClientConn
}

// New creates a client for the given master addresses. chunkSize must match the masters'.
func New(masters []string, chunkSize uint64) *Client {
	if chunkSize == 0 {
		chunkSize = 64 << 20
	}
	return &Client{masters: masters, chunkSize: chunkSize, retryFor: 5 * time.Second, conns: map[string]*grpc.ClientConn{}}
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cc := range c.conns {
		cc.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cc, ok := c.conns[addr]
	if !ok {
		var err error
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20), grpc.MaxCallSendMsgSize(256<<20)))
		if err != nil {
			return nil, err
		}
		c.conns[addr] = cc
	}
	return cc, nil
}

// withLeader runs fn against the master leader, following redirects and retrying
// across leader elections until retryFor elapses.
func (c *Client) withLeader(ctx context.Context, fn func(context.Context, pb.MasterClient) error) error {
	deadline := time.Now().Add(c.retryFor)
	var last error
	failed := map[string]bool{} // masters that just failed; skipped while others remain
	rr := 0
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		c.mu.Lock()
		target := c.leader
		c.mu.Unlock()
		if target == "" || failed[target] {
			if len(failed) >= len(c.masters) {
				failed = map[string]bool{}
			}
			for {
				target = c.masters[rr%len(c.masters)]
				rr++
				if !failed[target] {
					break
				}
			}
		}
		cc, err := c.conn(target)
		if err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = fn(cctx, pb.NewMasterClient(cc))
		cancel()
		if err == nil {
			c.mu.Lock()
			c.leader = target
			c.mu.Unlock()
			return nil
		}
		st, _ := status.FromError(err)
		switch {
		case st.Code() == codes.FailedPrecondition && strings.HasPrefix(st.Message(), master.NotLeaderPrefix):
			hint := strings.TrimPrefix(st.Message(), master.NotLeaderPrefix)
			// A live master vouching for a leader outranks our earlier failure with it
			// (it may have timed out mid-election and has since won).
			delete(failed, hint)
			c.mu.Lock()
			c.leader = hint // may be "" during an election
			c.mu.Unlock()
		case st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded:
			failed[target] = true
			c.mu.Lock()
			c.leader = ""
			c.mu.Unlock()
		default:
			return err // real error (not found, exists, ...)
		}
		last = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(time.Duration(min(2+attempt*2, 50)) * time.Millisecond)
	}
	return fmt.Errorf("no master leader reachable: %w", last)
}

// Leader returns the currently known leader address (after at least one call).
func (c *Client) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

// Put stores data as a new immutable file.
func (c *Client) Put(ctx context.Context, path string, data []byte) error {
	if err := c.withLeader(ctx, func(ctx context.Context, m pb.MasterClient) error {
		_, err := m.CreateFile(ctx, &pb.CreateFileRequest{Path: path})
		return err
	}); err != nil {
		return err
	}
	for off := uint64(0); off < uint64(len(data)) || (off == 0 && len(data) == 0); off += c.chunkSize {
		end := min(off+c.chunkSize, uint64(len(data)))
		piece := data[off:end]
		var loc *pb.ChunkLocation
		if err := c.withLeader(ctx, func(ctx context.Context, m pb.MasterClient) error {
			r, err := m.AllocateChunk(ctx, &pb.AllocateChunkRequest{Path: path})
			if err == nil {
				loc = r.Chunk
			}
			return err
		}); err != nil {
			return err
		}
		cc, err := c.conn(loc.Replicas[0])
		if err != nil {
			return err
		}
		if _, err := pb.NewChunkServerClient(cc).WriteChunk(ctx, &pb.WriteChunkRequest{
			Handle: loc.Handle, Data: piece, Checksum: chunkserver.Checksum(piece), ForwardTo: loc.Replicas[1:],
		}); err != nil {
			return fmt.Errorf("write chunk %d: %w", loc.Handle, err)
		}
		if err := c.withLeader(ctx, func(ctx context.Context, m pb.MasterClient) error {
			_, err := m.CommitChunk(ctx, &pb.CommitChunkRequest{Path: path, Handle: loc.Handle, Size: uint64(len(piece))})
			return err
		}); err != nil {
			return err
		}
		if len(data) == 0 {
			break
		}
	}
	return nil
}

// Stat returns file metadata and chunk locations.
func (c *Client) Stat(ctx context.Context, path string) (*pb.GetFileResponse, error) {
	var info *pb.GetFileResponse
	err := c.withLeader(ctx, func(ctx context.Context, m pb.MasterClient) error {
		r, err := m.GetFile(ctx, &pb.GetFileRequest{Path: path})
		info = r
		return err
	})
	return info, err
}

// ErrNoReplica is returned when every replica of a chunk failed.
var ErrNoReplica = errors.New("no healthy replica")

// Get reads a whole file; chunks are fetched in parallel, each from a random healthy replica.
func (c *Client) Get(ctx context.Context, path string) ([]byte, error) {
	info, err := c.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	return c.ReadChunks(ctx, info)
}

// ReadChunks fetches the chunks described by a Stat result (lets callers cache metadata).
func (c *Client) ReadChunks(ctx context.Context, info *pb.GetFileResponse) ([]byte, error) {
	out := make([]byte, info.Size)
	offsets := make([]uint64, len(info.Chunks))
	var off uint64
	for i, ch := range info.Chunks {
		offsets[i] = off
		off += ch.Size
	}
	errs := make([]error, len(info.Chunks))
	var wg sync.WaitGroup
	for i, ch := range info.Chunks {
		wg.Add(1)
		go func(i int, ch *pb.ChunkLocation) {
			defer wg.Done()
			data, err := c.readChunk(ctx, ch)
			if err == nil && uint64(len(data)) != ch.Size {
				err = fmt.Errorf("chunk %d: got %d bytes, want %d", ch.Handle, len(data), ch.Size)
			}
			if err != nil {
				errs[i] = err
				return
			}
			copy(out[offsets[i]:], data)
		}(i, ch)
	}
	wg.Wait()
	return out, errors.Join(errs...)
}

func (c *Client) readChunk(ctx context.Context, ch *pb.ChunkLocation) ([]byte, error) {
	if len(ch.Replicas) == 0 {
		return nil, fmt.Errorf("chunk %d: %w (no live replicas)", ch.Handle, ErrNoReplica)
	}
	start := rand.Intn(len(ch.Replicas))
	var errs []error
	for k := range ch.Replicas {
		addr := ch.Replicas[(start+k)%len(ch.Replicas)]
		cc, err := c.conn(addr)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		r, err := pb.NewChunkServerClient(cc).ReadChunk(rctx, &pb.ReadChunkRequest{Handle: ch.Handle})
		cancel()
		if err == nil {
			return r.Data, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err)) // corrupt or down: try the next replica
	}
	return nil, fmt.Errorf("chunk %d: %w: %v", ch.Handle, ErrNoReplica, errors.Join(errs...))
}

func (c *Client) Delete(ctx context.Context, path string) error {
	return c.withLeader(ctx, func(ctx context.Context, m pb.MasterClient) error {
		_, err := m.DeleteFile(ctx, &pb.DeleteFileRequest{Path: path})
		return err
	})
}

func (c *Client) List(ctx context.Context, prefix string) ([]*pb.FileInfo, error) {
	var files []*pb.FileInfo
	err := c.withLeader(ctx, func(ctx context.Context, m pb.MasterClient) error {
		r, err := m.ListFiles(ctx, &pb.ListFilesRequest{Prefix: prefix})
		if err == nil {
			files = r.Files
		}
		return err
	})
	return files, err
}
