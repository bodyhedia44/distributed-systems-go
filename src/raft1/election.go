package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) startElection() {
	if rf.isLeader {
		return
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeard = time.Now()
	rf.persist()
	term := rf.currentTerm
	votes := 1

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}

			if rf.currentTerm != term || !reply.VoteGranted {
				return
			}

			votes++
			if votes == len(rf.peers)/2+1 {
				rf.isLeader = true
				rf.lastHeard = time.Now()
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
				}
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.matchIndex {
					rf.matchIndex[i] = 0
				}
				go rf.sendheartbeat()
			}
		}(i)
	}
}

func (rf *Raft) sendheartbeat() {
	for {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.replicateAll()

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		if !rf.isLeader && time.Since(rf.lastHeard) >= time.Duration(150+rand.Int63()%200)*time.Millisecond {
			rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
