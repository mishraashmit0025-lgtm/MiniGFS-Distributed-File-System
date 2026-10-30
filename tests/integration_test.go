package tests

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/client"
	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/cluster"
)

const chunkSize = 64 << 10 // small chunks so tests exercise multi-chunk files

func start(t *testing.T, masters, chunkservers int) (*cluster.Cluster, *client.Client) {
	t.Helper()
	c, err := cluster.Start(cluster.Options{
		Masters: masters, ChunkServers: chunkservers, ChunkSize: chunkSize, Replication: 3, Dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	cl := client.New(c.MasterAddrs, chunkSize)
	t.Cleanup(func() {
		cl.Close()
		c.Stop()
	})
	return c, cl
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func TestPutGetRoundTrip(t *testing.T) {
	_, cl := start(t, 3, 4)
	ctx := context.Background()
	for _, size := range []int{0, 1, chunkSize - 1, chunkSize, 5*chunkSize + 123} {
		data := randomBytes(size)
		path := "/data/file-" + time.Now().Format("150405.000000000")
		if err := cl.Put(ctx, path, data); err != nil {
			t.Fatalf("put %d bytes: %v", size, err)
		}
		got, err := cl.Get(ctx, path)
		if err != nil {
			t.Fatalf("get %d bytes: %v", size, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("round trip of %d bytes corrupted", size)
		}
		info, _ := cl.Stat(ctx, path)
		for _, ch := range info.Chunks {
			if len(ch.Replicas) != 3 {
				t.Fatalf("chunk %d has %d replicas, want 3", ch.Handle, len(ch.Replicas))
			}
		}
	}
}

func TestNamespaceOps(t *testing.T) {
	_, cl := start(t, 3, 3)
	ctx := context.Background()
	for _, p := range []string{"/a/1", "/a/2", "/b/1"} {
		if err := cl.Put(ctx, p, []byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := cl.Put(ctx, "/a/1", []byte("x")); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate create: got %v", err)
	}
	files, _ := cl.List(ctx, "/a/")
	if len(files) != 2 || files[0].Path != "/a/1" || files[1].Path != "/a/2" {
		t.Fatalf("list /a/: %v", files)
	}
	if err := cl.Delete(ctx, "/a/1"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Get(ctx, "/a/1"); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted file: got %v", err)
	}
}

func TestMasterFailover(t *testing.T) {
	c, cl := start(t, 3, 3)
	ctx := context.Background()
	data := randomBytes(3 * chunkSize)
	if err := cl.Put(ctx, "/before", data); err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	c.KillMaster(old)
	killed := time.Now()
	for c.Leader() < 0 {
		if time.Since(killed) > 5*time.Second {
			t.Fatal("no new leader")
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("new leader elected %v after the old one crashed", time.Since(killed))

	got, err := cl.Get(ctx, "/before") // committed data survives
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read after failover: %v", err)
	}
	if err := cl.Put(ctx, "/after", []byte("written by the new leader")); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
}

func TestChunkserverFailureAndRereplication(t *testing.T) {
	c, cl := start(t, 3, 4)
	ctx := context.Background()
	data := randomBytes(4 * chunkSize)
	if err := cl.Put(ctx, "/durable", data); err != nil {
		t.Fatal(err)
	}
	info, _ := cl.Stat(ctx, "/durable")
	victim := -1
	for i, a := range c.ChunkAddrs {
		if a == info.Chunks[0].Replicas[0] {
			victim = i
		}
	}
	c.KillChunkServer(victim)

	got, err := cl.Get(ctx, "/durable") // reads fail over to the remaining replicas
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read with a dead replica: %v", err)
	}
	// the master notices the dead server and restores 3 live replicas per chunk
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := cl.Stat(ctx, "/durable")
		ok := err == nil
		for _, ch := range info.GetChunks() {
			ok = ok && len(ch.Replicas) == 3
			for _, r := range ch.Replicas {
				ok = ok && r != c.ChunkAddrs[victim]
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("chunks not re-replicated: %v", info)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestChecksumDetectsCorruption(t *testing.T) {
	c, cl := start(t, 1, 3)
	ctx := context.Background()
	data := randomBytes(1000)
	if err := cl.Put(ctx, "/fragile", data); err != nil {
		t.Fatal(err)
	}
	info, _ := cl.Stat(ctx, "/fragile")
	h := info.Chunks[0].Handle
	// corrupt two of three replicas: reads must still return the correct bytes
	var corrupted int
	for i, a := range c.ChunkAddrs {
		if corrupted < 2 && contains(info.Chunks[0].Replicas, a) {
			if err := c.CorruptChunk(i, h); err != nil {
				t.Fatal(err)
			}
			corrupted++
		}
	}
	for k := 0; k < 10; k++ {
		got, err := cl.Get(ctx, "/fragile")
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("read with corrupted replicas: %v", err)
		}
	}
	// corrupt the last one too: the read must fail loudly, never return bad data
	for i, a := range c.ChunkAddrs {
		if contains(info.Chunks[0].Replicas, a) {
			_ = c.CorruptChunk(i, h)
		}
	}
	// (the first two were flipped twice, restoring them; flip those again)
	corrupted = 0
	for i, a := range c.ChunkAddrs {
		if corrupted < 2 && contains(info.Chunks[0].Replicas, a) {
			_ = c.CorruptChunk(i, h)
			corrupted++
		}
	}
	if _, err := cl.Get(ctx, "/fragile"); !errors.Is(err, client.ErrNoReplica) {
		t.Fatalf("all replicas corrupt: want ErrNoReplica, got %v", err)
	}
}

func TestConcurrentReads(t *testing.T) {
	c, cl := start(t, 3, 4)
	ctx := context.Background()
	data := randomBytes(2*chunkSize + 17)
	if err := cl.Put(ctx, "/hot", data); err != nil {
		t.Fatal(err)
	}
	info, err := cl.Stat(ctx, "/hot")
	if err != nil {
		t.Fatal(err)
	}
	const readers = 10000
	var failed atomic.Int64
	lat := make([]time.Duration, readers)
	var wg sync.WaitGroup
	t0 := time.Now()
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := time.Now()
			got, err := cl.ReadChunks(ctx, info)
			lat[i] = time.Since(s)
			if err != nil || !bytes.Equal(got, data) {
				failed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(t0)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("%d concurrent reads of a %d-byte, %d-chunk file across %d chunkservers: %v total, p50 %v, p99 %v",
		readers, len(data), len(info.Chunks), len(c.ChunkAddrs), elapsed, lat[readers/2], lat[readers*99/100])
	if failed.Load() != 0 {
		t.Fatalf("%d of %d concurrent reads failed", failed.Load(), readers)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
