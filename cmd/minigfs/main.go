// Command minigfs runs MiniGFS servers and client operations.
//
//	minigfs master      -addr 127.0.0.1:7001 -peers 127.0.0.1:7002,127.0.0.1:7003 -data ./m1
//	minigfs chunkserver -addr 127.0.0.1:8001 -masters 127.0.0.1:7001,... -data ./cs1
//	minigfs put   -masters ... local.bin /remote/path
//	minigfs get   -masters ... /remote/path out.bin
//	minigfs ls    -masters ... [prefix]
//	minigfs rm    -masters ... /remote/path
//	minigfs status -masters ...
//	minigfs bench [-failovers 20] [-readers 1000]      (in-process cluster)
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/client"
	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/chunkserver"
	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/cluster"
	"github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/internal/master"
	pb "github.com/mishraashmit0025-lgtm/MiniGFS-Distributed-File-System/proto/gfspb"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "master":
		err = runMaster(args)
	case "chunkserver":
		err = runChunkServer(args)
	case "put", "get", "ls", "rm", "status":
		err = runClient(cmd, args)
	case "bench":
		err = runBench(args)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: minigfs {master|chunkserver|put|get|ls|rm|status|bench} [flags]")
	os.Exit(2)
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
}

func runMaster(args []string) error {
	fs := flag.NewFlagSet("master", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:7001", "listen/advertise address")
	peers := fs.String("peers", "", "comma-separated addresses of the other masters")
	data := fs.String("data", "", "directory for Raft state (empty = in-memory)")
	chunk := fs.Uint64("chunk-size", 64<<20, "chunk size in bytes")
	repl := fs.Int("replication", 3, "replicas per chunk")
	_ = fs.Parse(args)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	m, err := master.New(master.Config{Address: *addr, Peers: split(*peers), DataDir: *data, ChunkSize: *chunk, Replication: *repl})
	if err != nil {
		return err
	}
	go func() { _ = m.Serve(lis) }()
	fmt.Printf("master %s up (peers %v)\n", *addr, split(*peers))
	waitForSignal()
	m.Stop()
	return nil
}

func runChunkServer(args []string) error {
	fs := flag.NewFlagSet("chunkserver", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8001", "listen/advertise address")
	masters := fs.String("masters", "127.0.0.1:7001", "comma-separated master addresses")
	data := fs.String("data", "./chunks", "chunk storage directory")
	_ = fs.Parse(args)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	s, err := chunkserver.New(chunkserver.Config{Address: *addr, Dir: *data, Masters: split(*masters)})
	if err != nil {
		return err
	}
	go func() { _ = s.Serve(lis) }()
	fmt.Printf("chunkserver %s up, storing in %s\n", *addr, *data)
	waitForSignal()
	s.Stop()
	return nil
}

func runClient(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	masters := fs.String("masters", "127.0.0.1:7001", "comma-separated master addresses")
	chunk := fs.Uint64("chunk-size", 64<<20, "chunk size in bytes (must match masters)")
	_ = fs.Parse(args)
	rest := fs.Args()
	ctx := context.Background()
	cl := client.New(split(*masters), *chunk)
	defer cl.Close()
	switch cmd {
	case "put":
		if len(rest) != 2 {
			return fmt.Errorf("usage: put <local-file> <remote-path>")
		}
		data, err := os.ReadFile(rest[0])
		if err != nil {
			return err
		}
		t0 := time.Now()
		if err := cl.Put(ctx, rest[1], data); err != nil {
			return err
		}
		fmt.Printf("stored %d bytes at %s in %v\n", len(data), rest[1], time.Since(t0).Round(time.Millisecond))
	case "get":
		if len(rest) != 2 {
			return fmt.Errorf("usage: get <remote-path> <local-file>")
		}
		data, err := cl.Get(ctx, rest[0])
		if err != nil {
			return err
		}
		return os.WriteFile(rest[1], data, 0o644)
	case "ls":
		prefix := "/"
		if len(rest) > 0 {
			prefix = rest[0]
		}
		files, err := cl.List(ctx, prefix)
		if err != nil {
			return err
		}
		for _, f := range files {
			fmt.Printf("%12d  %3d chunks  %s\n", f.Size, f.Chunks, f.Path)
		}
	case "rm":
		if len(rest) != 1 {
			return fmt.Errorf("usage: rm <remote-path>")
		}
		return cl.Delete(ctx, rest[0])
	case "status":
		for _, m := range split(*masters) {
			cc, err := grpc.NewClient(m, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			sctx, cancel := context.WithTimeout(ctx, time.Second)
			st, err := pb.NewMasterClient(cc).Status(sctx, &pb.StatusRequest{})
			cancel()
			cc.Close()
			if err != nil {
				fmt.Printf("%-22s unreachable (%v)\n", m, err)
				continue
			}
			role := "follower"
			if st.Leader {
				role = "LEADER"
			}
			fmt.Printf("%-22s %-8s term=%d commit=%d chunkservers=%v\n", m, role, st.Term, st.CommitIndex, st.LiveChunkservers)
		}
	}
	return nil
}

func percentile(d []time.Duration, p float64) time.Duration {
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(len(s)-1, int(p*float64(len(s))))]
}

// runBench measures leader failover time and concurrent read throughput on an in-process cluster.
func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	failovers := fs.Int("failovers", 20, "leader crash trials")
	readers := fs.Int("readers", 1000, "concurrent readers")
	size := fs.Int("size", 1<<20, "file size for the read benchmark")
	chunk := fs.Uint64("chunk-size", 256<<10, "chunk size")
	_ = fs.Parse(args)
	ctx := context.Background()

	var times, elections []time.Duration
	for i := 0; i < *failovers; i++ {
		dir, _ := os.MkdirTemp("", "minigfs-bench")
		c, err := cluster.Start(cluster.Options{Masters: 3, ChunkServers: 3, ChunkSize: *chunk, Dir: dir})
		if err != nil {
			return err
		}
		cl := client.New(c.MasterAddrs, *chunk)
		if err := cl.Put(ctx, "/probe", []byte("x")); err != nil {
			return err
		}
		c.KillMaster(c.Leader())
		t0 := time.Now()
		elected := make(chan time.Duration, 1)
		go func() {
			for c.Leader() < 0 {
				time.Sleep(200 * time.Microsecond)
			}
			elected <- time.Since(t0)
		}()
		// failover is complete when a client write succeeds against the new leader
		if err := cl.Put(ctx, fmt.Sprintf("/after-%d", i), []byte("y")); err != nil {
			return err
		}
		times = append(times, time.Since(t0))
		elections = append(elections, <-elected)
		cl.Close()
		c.Stop()
		os.RemoveAll(dir)
	}
	if len(times) > 0 {
		fmt.Printf("leader election (crash -> new leader), %d trials: p50 %v  p95 %v  max %v\n",
			len(elections), percentile(elections, .5).Round(time.Millisecond), percentile(elections, .95).Round(time.Millisecond),
			percentile(elections, 1).Round(time.Millisecond))
		fmt.Printf("leader failover (crash -> first successful write), %d trials: p50 %v  p95 %v  max %v\n",
			len(times), percentile(times, .5).Round(time.Millisecond), percentile(times, .95).Round(time.Millisecond),
			percentile(times, 1).Round(time.Millisecond))
	}

	if *readers > 0 {
		dir, _ := os.MkdirTemp("", "minigfs-bench")
		defer os.RemoveAll(dir)
		c, err := cluster.Start(cluster.Options{Masters: 3, ChunkServers: 4, ChunkSize: *chunk, Dir: dir})
		if err != nil {
			return err
		}
		defer c.Stop()
		cl := client.New(c.MasterAddrs, *chunk)
		defer cl.Close()
		data := make([]byte, *size)
		_, _ = rand.Read(data)
		if err := cl.Put(ctx, "/hot", data); err != nil {
			return err
		}
		info, err := cl.Stat(ctx, "/hot")
		if err != nil {
			return err
		}
		lat := make([]time.Duration, *readers)
		var failed atomic.Int64
		var wg sync.WaitGroup
		t0 := time.Now()
		for i := 0; i < *readers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				s := time.Now()
				if _, err := cl.ReadChunks(ctx, info); err != nil {
					failed.Add(1)
				}
				lat[i] = time.Since(s)
			}(i)
		}
		wg.Wait()
		el := time.Since(t0)
		fmt.Printf("%d concurrent reads of a %d KiB file (%d chunks, 4 chunkservers): %d failed, %.0f reads/s, %.0f MiB/s, p50 %v, p99 %v\n",
			*readers, *size>>10, len(info.Chunks), failed.Load(), float64(*readers)/el.Seconds(),
			float64(*readers)*float64(*size)/el.Seconds()/(1<<20),
			percentile(lat, .5).Round(time.Millisecond), percentile(lat, .99).Round(time.Millisecond))
	}
	return nil
}
