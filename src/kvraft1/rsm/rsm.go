package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	idCounter    int
	waiters      map[int]*waiter
}

type waiter struct {
	id int
	ch chan waiterResult
}
type waiterResult struct {
	id  int
	rep any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiters:      make(map[int]*waiter),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	if data := persister.ReadSnapshot(); len(data) > 0 {
		sm.Restore(data)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
			continue
		}
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		rep := rsm.sm.DoOp(op.Req)

		rsm.mu.Lock()
		w, ok := rsm.waiters[msg.CommandIndex]
		rsm.mu.Unlock()

		if ok {
			w.ch <- waiterResult{id: op.Id, rep: rep}
		}

		if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
			rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
		}
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	rsm.idCounter++
	n := rsm.idCounter
	rsm.mu.Unlock()
	op := Op{Me: rsm.me, Id: n, Req: req}

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	w := &waiter{id: op.Id, ch: make(chan waiterResult, 1)}

	rsm.mu.Lock()
	rsm.waiters[index] = w
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.waiters, index)
		rsm.mu.Unlock()
	}()

	// a stale entry at index may be truncated rather than overwritten, so
	// nothing ever arrives on w.ch; poll for a term change to detect that.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case res := <-w.ch:
			if res.id != op.Id {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, res.rep
		case <-ticker.C:
			if curTerm, isLeader := rsm.rf.GetState(); !isLeader || curTerm != term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
