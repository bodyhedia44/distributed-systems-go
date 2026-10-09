package raft

// RPC Handlers and argument/reply structures for Raft.

import "time"

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// optimization so the leader can back up nextIndex by more than one
	// entry per rejected AppendEntries (see Raft student guide).
	ConflictIndex int
	ConflictTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
		rf.persist()
	}

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.termAt(lastLogIndex)
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.lastHeard = time.Now()
		rf.persist()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

// example AppendEntries RPC handler; carries heartbeats and log entries.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.isLeader = false
	rf.lastHeard = time.Now()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex && rf.termAt(idx-1) == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	insertAt := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		logIdx := insertAt + i
		if logIdx > rf.lastLogIndex() {
			break
		}
		if rf.termAt(logIdx) != args.Entries[i].Term {
			rf.log = rf.log[:logIdx-rf.lastIncludedIndex]
			break
		}
	}
	rf.log = append(rf.log, args.Entries[i:]...)
	if i < len(args.Entries) {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
		rf.applyCond.Signal()
	}

	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
