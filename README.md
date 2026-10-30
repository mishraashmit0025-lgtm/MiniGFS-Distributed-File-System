# MiniGFS

A small distributed file system in Go, modeled on the Google File System paper. Metadata lives on a group of masters that agree on every change through my own Raft implementation. File data is split into chunks and stored with 3-way replication on chunkservers, which checksum everything they hold.

I wrote it to understand what actually happens inside systems like GFS and HDFS when a machine dies halfway through a write.

## What it does

- **Raft from scratch** (`internal/raft`): randomized elections, log replication with fast conflict back-off, majority commit restricted to the current term, and term/vote/log persisted to disk before any reply that depends on them. No Raft library is used.
- **Replicated master**: namespace operations (create, delete, allocate chunk, commit chunk) go through the Raft log, so any master can take over after a crash without losing metadata.
- **Chunkservers** store chunks on disk with CRC-32C checksums, send heartbeats to the masters, and forward writes down a replica pipeline.
- **Self-healing**: the leader notices when a chunk drops below its replication factor and copies it from a surviving replica to a new server.
- **Corruption detection**: a chunkserver refuses to serve a chunk that fails its checksum, and the client falls back to another replica.
- **Client library and CLI** with automatic leader discovery and retry, so a master failover is invisible to callers apart from latency.

```
            +-----------+   Raft   +-----------+   Raft   +-----------+
            |  master 1 |<-------->|  master 2 |<-------->|  master 3 |
            +-----------+          +-----------+          +-----------+
                  ^  metadata (gRPC)        ^ heartbeats
                  |                         |
   +--------+     |       +---------------+---------------+
   | client |-----+------>| chunkserver A | chunkserver B | chunkserver C ...
   +--------+  chunk data +---------------+---------------+
```

## Numbers

Measured on a laptop (Windows 11, everything in one process over loopback) with `minigfs bench`:

| Scenario | Result |
|---|---|
| Leader crash to new leader elected (20 trials) | p50 92 ms, p95 247 ms |
| Leader crash to first successful client write (20 trials) | p50 146 ms, p95 308 ms |
| 1,000 concurrent reads of a 1 MiB file, 4 chunkservers | 0 failures, about 1 GiB/s aggregate |

These are loopback numbers, so they show the protocol overhead and not network cost.

## Running it

Requires Go 1.27 or newer (see go.mod).

```bash
go build -o minigfs ./cmd/minigfs

# three masters
./minigfs master -addr 127.0.0.1:7001 -peers 127.0.0.1:7002,127.0.0.1:7003 -data ./m1 &
./minigfs master -addr 127.0.0.1:7002 -peers 127.0.0.1:7001,127.0.0.1:7003 -data ./m2 &
./minigfs master -addr 127.0.0.1:7003 -peers 127.0.0.1:7001,127.0.0.1:7002 -data ./m3 &

# three chunkservers
M=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
for p in 8001 8002 8003; do ./minigfs chunkserver -addr 127.0.0.1:$p -masters $M -data ./cs$p & done

./minigfs put    -masters $M ./README.md /docs/readme
./minigfs ls     -masters $M /docs
./minigfs get    -masters $M /docs/readme out.md
./minigfs status -masters $M
```

Kill whichever master `status` reports as leader and run `get` again; it still works.

The benchmark spins up its own in-process cluster:

```bash
./minigfs bench -failovers 20 -readers 1000
```

## Tests

```bash
go test -race ./...
```

The Raft tests cover single-leader election, in-order replication, leader failover, a minority partition that must not commit, and recovery from disk after a restart. The integration tests start real clusters and check put/get round trips, namespace operations, master failover in the middle of use, chunkserver loss followed by re-replication, checksum detection of a deliberately corrupted chunk, and concurrent readers.

## Layout

```
cmd/minigfs          CLI: servers, client commands, benchmark
client/              client library (leader discovery, chunked put/get)
internal/raft        Raft consensus
internal/master      master gRPC service and replicated state machine
internal/chunkserver chunk storage, checksums, replication pipeline
internal/cluster     in-process cluster used by tests and the benchmark
proto/               protobuf definitions (go generate ./proto to rebuild)
tests/               end-to-end tests
```

## Simplifications

Files are immutable once written (no record append or random writes), there is no snapshotting or log compaction in Raft, and there is no authentication. Those are the next things I would add.
