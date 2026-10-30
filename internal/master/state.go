// Package master implements the MiniGFS metadata server. The namespace and
// chunk metadata form a deterministic state machine replicated with Raft;
// chunk *locations* are soft state rebuilt from chunkserver heartbeats, as in GFS.
package master

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Command is one replicated state-machine operation.
type Command struct {
	Op       string   `json:"op"` // create | delete | alloc | commit | addreplica
	Path     string   `json:"path,omitempty"`
	Handle   uint64   `json:"handle,omitempty"`
	Size     uint64   `json:"size,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
}

type File struct {
	Chunks []uint64
	Size   uint64
}

type Chunk struct {
	Version  uint64
	Size     uint64
	Replicas []string // servers the chunk was placed on (primary first)
	Path     string
	Done     bool // committed: all replicas acknowledged the write
}

// State is the replicated metadata. All mutation goes through Apply.
type State struct {
	mu         sync.RWMutex
	Files      map[string]*File
	Chunks     map[uint64]*Chunk
	NextHandle uint64
}

func NewState() *State {
	return &State{Files: map[string]*File{}, Chunks: map[uint64]*Chunk{}, NextHandle: 1}
}

// Result of applying a command.
type Result struct {
	Handle uint64
	Err    string
}

// Apply is the Raft state-machine callback; it must be deterministic.
func (s *State) Apply(raw []byte) any {
	var c Command
	if err := json.Unmarshal(raw, &c); err != nil {
		return Result{Err: "bad command: " + err.Error()}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch c.Op {
	case "create":
		if _, ok := s.Files[c.Path]; ok {
			return Result{Err: fmt.Sprintf("file exists: %s", c.Path)}
		}
		s.Files[c.Path] = &File{}
	case "delete":
		f, ok := s.Files[c.Path]
		if !ok {
			return Result{Err: fmt.Sprintf("no such file: %s", c.Path)}
		}
		for _, h := range f.Chunks {
			delete(s.Chunks, h) // chunkservers learn via heartbeat replies and garbage-collect
		}
		delete(s.Files, c.Path)
	case "alloc":
		f, ok := s.Files[c.Path]
		if !ok {
			return Result{Err: fmt.Sprintf("no such file: %s", c.Path)}
		}
		h := s.NextHandle
		s.NextHandle++
		s.Chunks[h] = &Chunk{Version: 1, Replicas: append([]string(nil), c.Replicas...), Path: c.Path}
		f.Chunks = append(f.Chunks, h)
		return Result{Handle: h}
	case "commit":
		ch, ok := s.Chunks[c.Handle]
		if !ok || ch.Path != c.Path {
			return Result{Err: fmt.Sprintf("unknown chunk %d for %s", c.Handle, c.Path)}
		}
		if !ch.Done {
			ch.Done, ch.Size = true, c.Size
			s.Files[c.Path].Size += c.Size
		}
	case "addreplica":
		ch, ok := s.Chunks[c.Handle]
		if !ok {
			return Result{Err: "unknown chunk"}
		}
		for _, r := range c.Replicas {
			if !contains(ch.Replicas, r) {
				ch.Replicas = append(ch.Replicas, r)
			}
		}
		ch.Version++
	default:
		return Result{Err: "unknown op " + c.Op}
	}
	return Result{}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- read-only views

func (s *State) file(path string) (File, []Chunk, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.Files[path]
	if !ok {
		return File{}, nil, false
	}
	chunks := make([]Chunk, len(f.Chunks))
	for i, h := range f.Chunks {
		c := s.Chunks[h]
		chunks[i] = Chunk{Version: c.Version, Size: c.Size, Replicas: append([]string(nil), c.Replicas...), Path: c.Path, Done: c.Done}
	}
	return File{Chunks: append([]uint64(nil), f.Chunks...), Size: f.Size}, chunks, true
}

func (s *State) list(prefix string) []struct {
	Path   string
	Size   uint64
	Chunks int
} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []struct {
		Path   string
		Size   uint64
		Chunks int
	}
	for p, f := range s.Files {
		if strings.HasPrefix(p, prefix) {
			out = append(out, struct {
				Path   string
				Size   uint64
				Chunks int
			}{p, f.Size, len(f.Chunks)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func (s *State) hasChunk(h uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.Chunks[h]
	return ok
}

// committedChunks returns handle -> assigned replicas for all committed chunks.
func (s *State) committedChunks() map[uint64][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[uint64][]string, len(s.Chunks))
	for h, c := range s.Chunks {
		if c.Done {
			out[h] = append([]string(nil), c.Replicas...)
		}
	}
	return out
}
