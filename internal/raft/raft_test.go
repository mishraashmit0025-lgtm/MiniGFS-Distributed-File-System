package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type cluster struct {
	t     *testing.T
	net   *MemNetwork
	ids   []string
	nodes map[string]*Node
	mu    sync.Mutex
	apply map[string][]string // applied commands per node
}

func newCluster(t *testing.T, size int, dataDir string) *cluster {
	c := &cluster{t: t, net: NewMemNetwork(), nodes: map[string]*Node{}, apply: map[string][]string{}}
	for i := 0; i < size; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.start(id, dataDir)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) start(id, dataDir string) {
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	n, err := New(Config{
		ID: id, Peers: peers, Transport: c.net.Transport(id), DataDir: dataDir,
		Apply: func(cmd []byte) any {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.apply[id] = append(c.apply[id], string(cmd))
			return len(c.apply[id])
		},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
	c.net.Register(id, n)
	n.Start()
}

func (c *cluster) leader(timeout time.Duration, exclude ...string) (string, time.Duration) {
	t0 := time.Now()
	for time.Since(t0) < timeout {
		leaders := map[uint64][]string{}
		for id, n := range c.nodes {
			skip := false
			for _, e := range exclude {
				skip = skip || e == id
			}
			if skip {
				continue
			}
			if term, ok, _ := n.State(); ok {
				leaders[term] = append(leaders[term], id)
			}
		}
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("two leaders in term %d: %v", term, ls)
			}
		}
		var best uint64
		var who string
		for term, ls := range leaders {
			if term >= best {
				best, who = term, ls[0]
			}
		}
		if who != "" {
			return who, time.Since(t0)
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return "", 0
}

func (c *cluster) propose(cmd string) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			_, err := n.Propose(ctx, []byte(cmd))
			cancel()
			if err == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("could not commit %q", cmd)
}

func (c *cluster) waitApplied(id string, want int) []string {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := append([]string(nil), c.apply[id]...)
		c.mu.Unlock()
		if len(got) >= want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("%s applied %d entries, want %d", id, len(c.apply[id]), want)
	return nil
}

func TestElectsSingleLeader(t *testing.T) {
	c := newCluster(t, 3, "")
	c.leader(2 * time.Second)
	time.Sleep(200 * time.Millisecond)
	l1, _ := c.leader(time.Second)
	l2, _ := c.leader(time.Second)
	if l1 != l2 {
		t.Fatalf("leadership unstable without failures: %s then %s", l1, l2)
	}
}

func TestReplicatesInOrder(t *testing.T) {
	c := newCluster(t, 3, "")
	c.leader(2 * time.Second)
	for i := 0; i < 50; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i))
	}
	for _, id := range c.ids {
		got := c.waitApplied(id, 50)
		for i := 0; i < 50; i++ {
			if got[i] != fmt.Sprintf("cmd-%d", i) {
				t.Fatalf("%s: entry %d = %q", id, i, got[i])
			}
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, "")
	old, _ := c.leader(2 * time.Second)
	c.propose("before")
	c.net.SetDown(old, true)
	killed := time.Now()
	nl, _ := c.leader(2*time.Second, old)
	failover := time.Since(killed)
	t.Logf("failover took %v", failover)
	if nl == old {
		t.Fatal("isolated leader still leader")
	}
	c.propose("after")
	// old leader rejoins and catches up
	c.net.SetDown(old, false)
	got := c.waitApplied(old, 2)
	if got[0] != "before" || got[1] != "after" {
		t.Fatalf("rejoined node has %v", got)
	}
}

func TestMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 5, "")
	l, _ := c.leader(2 * time.Second)
	for _, id := range c.ids {
		if id != l {
			c.net.SetDown(id, true)
			break
		}
	}
	var others []string
	for _, id := range c.ids {
		if id != l {
			others = append(others, id)
		}
	}
	c.net.SetDown(others[1], true)
	c.net.SetDown(others[2], true) // leader + 1 of 5 left: no majority
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.nodes[l].Propose(ctx, []byte("x")); err == nil {
		t.Fatal("minority leader committed an entry")
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	c := newCluster(t, 3, dir)
	c.leader(2 * time.Second)
	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprintf("p%d", i))
	}
	for _, id := range c.ids {
		c.waitApplied(id, 10)
		c.nodes[id].Stop()
	}
	c.mu.Lock()
	c.apply = map[string][]string{}
	c.mu.Unlock()
	for _, id := range c.ids {
		c.start(id, dir)
	}
	c.leader(2 * time.Second)
	c.propose("p10")
	got := c.waitApplied("n0", 11)
	if got[0] != "p0" || got[10] != "p10" {
		t.Fatalf("log not restored: %v", got)
	}
}
