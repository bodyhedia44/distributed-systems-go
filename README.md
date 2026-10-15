## MIT 6.5840 — Distributed Systems Labs

This repository contains my solutions to the lab assignments for
[**MIT 6.5840: Distributed Systems**](https://pdos.csail.mit.edu/6.824/index.html)
(formerly 6.824), taught by the PDOS group at MIT.

All code is written in Go and builds on the course's provided skeleton/tester
code in `src/`.

### Labs implemented

| Lab | Topic | Path |
|---|---|---|
| 1 | MapReduce | `src/mr`, `src/mrapps` |
| 2 | Key/value server (`kvsrv1`) + lease-based lock | `src/kvsrv1` |
| 3 | Raft (leader election, log replication, persistence, snapshots) | `src/raft1` |
| 4 | Fault-tolerant key/value service (replicated state machine + kvraft) | `src/kvraft1` |
| 5 | Sharded key/value service (shard controller, shard groups, reconfiguration) | `src/shardkv1` |

### Building and testing

From `src/`, each lab can be built and tested with `make`, e.g.:

```sh
make mr            # Lab 1
make kvsrv1 lock1   # Lab 2
make raft1          # Lab 3
make rsm1 kvraft1   # Lab 4
make shardkv        # Lab 5
```

Use `RUN="-run <TestName>"` to run a specific test, for example:

```sh
make RUN="-run 5A" shardkv
```

### Collaboration policy

This is personal coursework. Per MIT 6.5840's
[collaboration policy](https://pdos.csail.mit.edu/6.824/labs/collab.html),
please don't copy this code if you are taking the course yourself.
