// Package cluster runs a complete MiniGFS deployment (masters + chunkservers)
// inside one process on loopback ports, for tests, benchmarks and demos.
package cluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/client"
	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/chunkserver"
	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/master"
)

type Options struct {
	Masters      int
	ChunkServers int
	ChunkSize    uint64
	Replication  int
	Dir          string // storage root; chunkservers use Dir/cs<N>, masters Dir/m<N>
}

type Cluster struct {
	Opts         Options
	MasterAddrs  []string
	ChunkAddrs   []string
	Masters      []*master.Master
	ChunkServers []*chunkserver.Server
	masterLis    []net.Listener
	chunkLis     []net.Listener
}

func listen() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

// Start launches the cluster and waits until a master leader knows about every chunkserver.
func Start(o Options) (*Cluster, error) {
	if o.Replication == 0 {
		o.Replication = 3
	}
	c := &Cluster{Opts: o}
	for i := 0; i < o.Masters; i++ {
		l, err := listen()
		if err != nil {
			return nil, err
		}
		c.masterLis = append(c.masterLis, l)
		c.MasterAddrs = append(c.MasterAddrs, l.Addr().String())
	}
	for i := 0; i < o.ChunkServers; i++ {
		l, err := listen()
		if err != nil {
			return nil, err
		}
		c.chunkLis = append(c.chunkLis, l)
		c.ChunkAddrs = append(c.ChunkAddrs, l.Addr().String())
	}
	for i, addr := range c.MasterAddrs {
		var peers []string
		for _, p := range c.MasterAddrs {
			if p != addr {
				peers = append(peers, p)
			}
		}
		m, err := master.New(master.Config{
			Address: addr, Peers: peers, DataDir: filepath.Join(o.Dir, fmt.Sprintf("m%d", i)),
			ChunkSize: o.ChunkSize, Replication: o.Replication,
			HeartbeatTimeout: 600 * time.Millisecond, RereplicateEvery: 200 * time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		c.Masters = append(c.Masters, m)
		go m.Serve(c.masterLis[i]) //nolint:errcheck // returns when stopped
	}
	for i, addr := range c.ChunkAddrs {
		s, err := chunkserver.New(chunkserver.Config{
			Address: addr, Dir: filepath.Join(o.Dir, fmt.Sprintf("cs%d", i)), Masters: c.MasterAddrs,
			HeartbeatInterval: 150 * time.Millisecond,
		})
		if err != nil {
			return nil, err
		}
		c.ChunkServers = append(c.ChunkServers, s)
		go s.Serve(c.chunkLis[i]) //nolint:errcheck
	}
	return c, c.WaitReady(10 * time.Second)
}

// Leader returns the index of the current master leader, or -1.
func (c *Cluster) Leader() int {
	for i, m := range c.Masters {
		if m != nil && m.IsLeader() {
			return i
		}
	}
	return -1
}

// WaitReady blocks until there is a leader that sees all running chunkservers.
func (c *Cluster) WaitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l >= 0 {
			cl := client.New(c.MasterAddrs, c.Opts.ChunkSize)
			st, err := statusOf(cl, c.MasterAddrs[l])
			cl.Close()
			if err == nil && len(st) >= c.runningChunkServers() {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("cluster not ready after %v", timeout)
}

func (c *Cluster) runningChunkServers() int {
	n := 0
	for _, s := range c.ChunkServers {
		if s != nil {
			n++
		}
	}
	return n
}

// KillMaster crashes master i.
func (c *Cluster) KillMaster(i int) {
	c.Masters[i].Stop()
	c.Masters[i] = nil
}

// KillChunkServer crashes chunkserver i (its data stays on disk).
func (c *Cluster) KillChunkServer(i int) {
	c.ChunkServers[i].Stop()
	c.ChunkServers[i] = nil
}

// CorruptChunk flips a byte in chunk handle on chunkserver i (for checksum tests).
func (c *Cluster) CorruptChunk(i int, handle uint64) error {
	p := filepath.Join(c.Opts.Dir, fmt.Sprintf("cs%d", i), fmt.Sprintf("%d.chunk", handle))
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	b[0] ^= 0xFF
	return os.WriteFile(p, b, 0o644)
}

func (c *Cluster) Stop() {
	for i := range c.ChunkServers {
		if c.ChunkServers[i] != nil {
			c.KillChunkServer(i)
		}
	}
	for i := range c.Masters {
		if c.Masters[i] != nil {
			c.KillMaster(i)
		}
	}
}
