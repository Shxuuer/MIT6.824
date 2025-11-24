package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    uint64
	Command interface{}
}

type RaftRole int

const (
	Follower RaftRole = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	log           []LogEntry
	role          RaftRole
	lastLogIndex  int
	lastLogTerm   int
	lastHeartbeat bool
	voteCount     int
}

func debug(format string, a ...interface{}) {
	if true {
		log.Printf(format, a...)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isLeader := rf.role == Leader
	// Your code here (3A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.lastHeartbeat = true
		rf.votedFor = args.CandidateId
		rf.role = Follower
		reply.VoteGranted = true
		debug("server %v voted for %v in term %v", rf.me, args.CandidateId, args.Term)
		return
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
	Ok   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Ok = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	rf.lastHeartbeat = true
	reply.Ok = true
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
	isLeader := rf.role == Leader

	// Your code here (3B).

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

type voteResult struct {
	reply RequestVoteReply
	ok    bool
}

func (rf *Raft) sendRequest(server int, ctx context.Context) <-chan voteResult {
	resultChan := make(chan voteResult)

	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}
	rf.mu.Unlock()

	go func() {
		var reply RequestVoteReply
		ok := rf.sendRequestVote(server, args, &reply)
		select {
		case <-ctx.Done():
			return
		case resultChan <- voteResult{reply: reply, ok: ok}:
		}
	}()

	return resultChan
}

func (rf *Raft) callVote(server int, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	select {
	case <-ctx.Done():
		return
	case result := <-rf.sendRequest(server, ctx):
		if result.ok {
			rf.mu.Lock()
			if result.reply.Term > rf.currentTerm {
				rf.currentTerm = result.reply.Term
				rf.votedFor = -1
				rf.role = Follower
				rf.mu.Unlock()
				return
			}
			if result.reply.VoteGranted {
				rf.voteCount += 1
			}
			rf.mu.Unlock()
		} else {
			debug("server %v failed to call RequestVote RPC to server %v", rf.me, server)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 500 and 700
		// milliseconds.
		ms := 400 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		if rf.role == Leader {
			continue
		}

		rf.mu.Lock()
		if !rf.lastHeartbeat {
			rf.role = Candidate
			rf.currentTerm = rf.currentTerm + 1
			debug("server %d starting election in term %d", rf.me, rf.currentTerm)
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()

			taskSize := len(rf.peers) - 1
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(100*time.Millisecond))
			defer cancel()
			var wg sync.WaitGroup
			wg.Add(taskSize)
			for i := 0; i < taskSize+1; i++ {
				if i == rf.me {
					continue
				}
				go rf.callVote(i, ctx, &wg)
			}
			wg.Wait()

			rf.mu.Lock()
			if rf.voteCount > len(rf.peers)/2 {
				rf.role = Leader
				rf.sendBeat()
				debug("server %d became leader in term %d, have %d", rf.me, rf.currentTerm, rf.voteCount)
			} else {
				rf.role = Follower
				debug("server %d failed to become leader in term %d, have %d", rf.me, rf.currentTerm, rf.voteCount)
			}
		}
		rf.lastHeartbeat = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// log.Printf("server %d sending heartbeat to server %d in term %d", rf.me, i, rf.currentTerm)
		go func() {
			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			rf.peers[i].Call("Raft.AppendEntries", args, &reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.role = Follower
			}
			rf.mu.Unlock()
		}()
	}
}

func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		time.Sleep(100 * time.Millisecond)
		if rf.role != Leader {
			continue
		}
		rf.mu.Lock()
		rf.sendBeat()
		rf.mu.Unlock()
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = Follower
	rf.lastHeartbeat = false
	rf.voteCount = 0

	rf.log = make([]LogEntry, 0)
	rf.lastLogIndex = -1
	rf.lastLogTerm = -1

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartbeat()

	return rf
}
