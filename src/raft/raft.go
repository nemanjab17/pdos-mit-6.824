package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	log           []LogEntry
	matchIndex    []int
	nextIndex     []int
	lastHeartbeat time.Time
	votes         int
	commitIndex   int
	lastApplied   int
	role          int // 0: follower, 1: candidate, 2: leader
	applyCh       chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
	Id	  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isleader = rf.role == 2
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) setFollower(withLock bool) {
	if withLock {
		rf.mu.Lock()
	}
	rf.role = 0
	rf.votedFor = -1
	rf.votes = 0
	rf.lastHeartbeat = time.Now()

	if withLock {
		rf.mu.Unlock()
	}
}

func (rf *Raft) setCandidate(withLock bool) {
	if withLock {
		rf.mu.Lock()
	}
	rf.role = 1
	rf.votedFor = rf.me
	rf.votes = 1
	rf.currentTerm++
	rf.lastHeartbeat = time.Now()
	if withLock {
		rf.mu.Unlock()
	}
}

func (rf *Raft) setLeader(withLock bool) {
	if withLock {
		rf.mu.Lock()
	}
	rf.role = 2
	rf.votedFor = -1
	rf.votes = 0
	rf.lastHeartbeat = time.Now()
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex()
	}
	if withLock {
		rf.mu.Unlock()
	}
	rf.sendAppendEntriesAll(false)
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == 2 {
			// send entries that have not been replicated
			// if there is no new entries, send empty entries
			// to prevent other servers from starting election
			rf.sendAppendEntriesAll(false)
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntriesAll(withLock bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if withLock {
			rf.mu.Lock()
		}
		go rf.sendAppendEntries(i)
		
		if withLock {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppendEntries(i int) {
	rf.mu.Lock()
	if rf.role != 2 {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	me := rf.me
	prevLogIndex, prevLogTerm := rf.getLastIdxTerm(i)
	
	// send all entries that have not been replicated to server i
	entries := make([]LogEntry, 0)
	if rf.lastLogIndex() >= rf.nextIndex[i] {
		entries = rf.log[rf.nextIndex[i]-1:]
	}

	rf.mu.Unlock()
	args := &AppendEntriesArgs{
		Term:     currentTerm,
		LeaderId: me,
		Entries:  entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	
	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if rf.role != 2 {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			// stale term, revert to follower
			rf.currentTerm = reply.Term
			rf.setFollower(false)
			rf.mu.Unlock()
			return
		}
		// if entries were sent, count number of replicated entries
		if len(entries) > 0 {
			if reply.Success {
				rf.matchIndex[i] = prevLogIndex + len(entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				// if majority of servers have replicated entries, commit
				// and send commit message to applyCh
				
				if rf.commitIndex < rf.matchIndex[i] {
					count := 1
					for j := range rf.peers {
						if j == rf.me {
							continue
						}
						if rf.matchIndex[j] > rf.commitIndex {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = rf.matchIndex[i]
						// apply all entries that have been committed
						rf.applyCommited()

					}
				}

			} else {
				// if log is not up to date, decrement nextIndex and retry
				if rf.nextIndex[i] > 1{
					rf.nextIndex[i]--
				}
			}
			
		}
		rf.mu.Unlock()
			
	} else {
		// failed to send, try again later
		time.Sleep(100 * time.Millisecond)
		go rf.sendAppendEntries(i)
	}
}

func (rf *Raft) getLastIdxTerm(i int) (int, int) {
	prevLogIndex := 0
	prevLogTerm := 0
	if rf.nextIndex[i] > 1 {
		prevLogIndex = rf.nextIndex[i] - 1
		prevLogTerm = rf.log[prevLogIndex - 1].Term
	}

	return prevLogIndex, prevLogTerm
}

func (rf *Raft) getPrevIdxTerm(i int) (int, int) {
	prevLogIndex := 0
	prevLogTerm := 0
	if len(rf.log) > 0 {
		prevLogIndex = len(rf.log) - 1
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	return prevLogIndex, prevLogTerm
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) + 1
}

func (rf *Raft) requestVote(i int) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	lastLogIndex, lastLogTerm := rf.getPrevIdxTerm(i)
	rf.mu.Unlock()
	args := &RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(i, args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.setFollower(false)
		} else if reply.VoteGranted {
			rf.votes++
			if rf.votes > len(rf.peers)/2 {
				rf.setLeader(false)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElect() {
	rf.setCandidate(true)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestVote(i)

		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	Entries  []LogEntry
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.lastHeartbeat = time.Now()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return
	}

	//heartbeat
	if len(args.Entries) == 0 {
		reply.Term = currentTerm
		reply.Success = true
		// update commit index
		rf.updateCommitIdx(args)
		return
	} else {
		rf.mu.Lock()

		if args.PrevLogIndex > len(rf.log) {
			// follower is missing entries, send back
			// the last term it has so that the leader can go back
			// and find where we match
			reply.Term = currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return
		}
		if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
			// follower has a different term at PrevLogIndex
			reply.Term = currentTerm
			reply.Success = false
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(args.Entries); i++ {
			// if the entry at the same index is different, delete it and all that follow
			if args.PrevLogIndex + i + 1 <= len(rf.log) && rf.log[args.PrevLogIndex + i].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex + i]
				break
			}
		}


		// append any new entries not already in the log
		for i := 0; i < len(args.Entries); i++ {
			if args.PrevLogIndex + i + 1 > len(rf.log) {
				rf.log = append(rf.log, args.Entries[i])
			}
		}


		rf.mu.Unlock()
		reply.Term = currentTerm
		reply.Success = true
		return


	}

}

func (rf *Raft) updateCommitIdx(args *AppendEntriesArgs) {
	rf.mu.Lock()

	// check if follower is up to date
	if args.PrevLogIndex > len(rf.log) {
		rf.mu.Unlock()
		return 
	}

	// check if follower has conflicting entry
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
		rf.mu.Unlock()
		return
	}

	if args.LeaderCommit > rf.commitIndex{
		if args.LeaderCommit < len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
		// apply all entries up to commit index
		rf.applyCommited()

	}
	rf.mu.Unlock()
}

func (rf *Raft) applyCommited() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied; i < rf.commitIndex; i++ {

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i + 1,
			}
		}
		rf.lastApplied = rf.commitIndex
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	rf.mu.Unlock()
	if args.Term < currentTerm {
		// this leader is stale
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > currentTerm {
		// this leader is newer, revert to follower
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.setFollower(false)
		rf.mu.Unlock()
	}
	if votedFor == -1 || votedFor == args.CandidateId {
		rf.mu.Lock()
		var lastLog LogEntry
		if len(rf.log) == 0 {
			lastLog = LogEntry{Term: 0, Command: nil}
		} else {
			lastLog = rf.log[len(rf.log)-1]
		}
		if args.LastLogTerm < lastLog.Term {
			// election restriction check
			reply.Term = currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}
		if args.LastLogTerm == lastLog.Term && args.LastLogIndex < len(rf.log)-1 {
			// election restriction check
			reply.Term = currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = currentTerm
		reply.VoteGranted = true
		rf.lastHeartbeat = time.Now() //reset heartbeat timer
		rf.mu.Unlock()
		return
	} else {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}

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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	
	// Your code here (2B).
	term = rf.currentTerm
	entries := make([]LogEntry, 1)
	entries[0] = LogEntry{term, command, rf.lastLogIndex() + 1}
	rf.mu.Lock()
	isLeader = rf.role == 2
	if isLeader {
		index = rf.lastLogIndex()
		rf.log = append(rf.log, entries...)
		rf.matchIndex[rf.me] = len(rf.log)
		rf.mu.Unlock()
		// rf.sendAppendEntriesAll(entries, index - 1, true)
	} else {
		rf.mu.Unlock()
	}
	
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		ms := 500 + (rand.Int63() % 350)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		last := rf.lastHeartbeat
		role := rf.role
		rf.mu.Unlock()
		now := time.Now()
		if now.Sub(last) > time.Duration(ms)*time.Millisecond && role != 2 {
			rf.startElect()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.lastHeartbeat = time.Now()
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.setFollower(false)
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()

	return rf
}
