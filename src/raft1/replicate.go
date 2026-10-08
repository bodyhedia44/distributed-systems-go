package raft

import "6.5840/raftapi"

func (rf *Raft) replicateAll() {
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateOne(i)
	}
}

func (rf *Raft) replicateOne(server int) {
	rf.mu.Lock()
	if !rf.isLeader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[server]-1 < rf.lastIncludedIndex {
		rf.installSnapshotOn(server)
		return
	}

	term := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex > rf.lastLogIndex() {
		prevLogIndex = rf.lastLogIndex()
	}
	prevLogTerm := rf.termAt(prevLogIndex)
	tail := rf.log[prevLogIndex-rf.lastIncludedIndex+1:]
	entries := make([]LogEntry, len(tail))
	copy(entries, tail)
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(server, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.isLeader = false
		rf.persist()
		return
	}

	if !rf.isLeader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		rf.matchIndex[server] = prevLogIndex + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.updateCommitIndex()
		return
	}

	if reply.ConflictTerm == -1 {
		rf.nextIndex[server] = reply.ConflictIndex
	} else {
		idx := -1
		for i := rf.lastLogIndex(); i >= rf.lastIncludedIndex; i-- {
			if rf.termAt(i) == reply.ConflictTerm {
				idx = i
				break
			}
		}
		if idx >= 0 {
			rf.nextIndex[server] = idx + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}
	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}
}

func (rf *Raft) updateCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		count := 1 // ourselves
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

// applier sends newly committed entries, or an installed snapshot, to
// applyCh in order. It's the only goroutine that writes to applyCh.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		if rf.pendingSnapshotData != nil {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnapshotData,
				SnapshotTerm:  rf.pendingSnapshotTerm,
				SnapshotIndex: rf.pendingSnapshotIndex,
			}
			rf.pendingSnapshotData = nil
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			continue
		}

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.entryAt(rf.lastApplied)
			index := rf.lastApplied
			rf.mu.Unlock()
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: index,
			}
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}
