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
	term := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex < 0 {
		prevLogIndex = 0
	}
	if prevLogIndex >= len(rf.log) {
		prevLogIndex = len(rf.log) - 1
	}
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := make([]LogEntry, len(rf.log[prevLogIndex+1:]))
	copy(entries, rf.log[prevLogIndex+1:])
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
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
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
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
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

// applier sends newly committed entries to applyCh in order.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
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
