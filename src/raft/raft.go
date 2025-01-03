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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const HeartbeatIntervalMs = 50
const ElectionMinTimeoutMs = 150
const ElectionMaxTimeoutMs = 200

func makeElectionTimeout() time.Duration {
	return time.Duration(ElectionMinTimeoutMs+rand.Intn(ElectionMaxTimeoutMs-ElectionMinTimeoutMs)) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func heapDown(h []int, i int) {
	for {
		l := 2*i + 1
		if l >= len(h) {
			break
		}
		// find the smallest child
		c := l
		if r := l + 1; r < len(h) && h[r] < h[l] {
			c = r
		}
		// swap with the smallest child if necessary
		if h[i] <= h[c] {
			break
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
}

func heapify(h []int) {
	for i := len(h)/2 - 1; i >= 0; i-- {
		heapDown(h, i)
	}
}

// find the k-th largest element in the array
func topK(nums []int, k int) int {
	h := make([]int, k)
	copy(h, nums[:k])
	heapify(h)
	for i := k; i < len(nums); i++ {
		if nums[i] > h[0] {
			h[0] = nums[i]
			heapDown(h, 0)
		}
	}
	return h[0]
}

func assert(condition bool, message string) {
	if !condition {
		log.Panic(message)
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leader         bool
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	commitCond     *sync.Cond
	replicateCond  []*sync.Cond

	currentTerm int
	votedFor    int
	// log[0] is dummy, log[0].Command is the logStartIndex
	// log[i] is the (logStartIndex+i)-th log entry, numbered from 1
	log []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) prevLogTerm() int {
	// log[0].Term is the term of the prev log entry
	return rf.log[0].Term
}

func (rf *Raft) prevLogIndex() int {
	// log[0].Command is index of the prev log entry
	return rf.log[0].Command.(int)
}

func (rf *Raft) firstLogIndex() int {
	return rf.prevLogIndex() + 1
}

func (rf *Raft) lastLogIndex() int {
	// log[0] is dummy
	return rf.prevLogIndex() + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) nextLogIndex() int {
	return rf.prevLogIndex() + len(rf.log)
}

func (rf *Raft) getLog(index int) *LogEntry {
	assert(rf.firstLogIndex() <= index && index <= rf.lastLogIndex(), "index out of range")
	return &rf.log[index-rf.prevLogIndex()]
}

func (rf *Raft) getLogFrom(from int) []LogEntry {
	assert(rf.firstLogIndex() <= from, "index out of range")
	if from >= rf.nextLogIndex() {
		return []LogEntry{}
	}
	return rf.log[from-rf.prevLogIndex():]
}

func (rf *Raft) copyLogFrom(from int) []LogEntry {
	assert(rf.firstLogIndex() <= from, "index out of range")
	return append([]LogEntry{}, rf.getLogFrom(from)...)
}

func (rf *Raft) getLogSlice(from int, to int) []LogEntry {
	assert(rf.firstLogIndex() <= from && to <= rf.nextLogIndex(), "index out of range")
	return rf.log[from-rf.prevLogIndex() : to-rf.prevLogIndex()]
}

func (rf *Raft) getLogIndex(index int) int {
	// local log index -> global log index
	assert(1 <= index && index <= len(rf.log), "index out of range")
	return rf.prevLogIndex() + index
}

func (rf *Raft) getLogTerm(index int) int {
	// Note: allow get term of dummy log entry
	assert(rf.prevLogIndex() <= index && index <= rf.lastLogIndex(), "index out of range")
	return rf.log[index-rf.prevLogIndex()].Term
}

func (rf *Raft) switchLeader(leader bool) {
	if rf.leader == leader {
		return
	}
	rf.leader = leader
	if leader {
		// initialize leader state
		// all followers are behind, send logs from the beginning
		nextLogIndex := rf.nextLogIndex()
		for i := range rf.peers {
			rf.nextIndex[i] = nextLogIndex
			rf.matchIndex[i] = 0
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HeartbeatIntervalMs * time.Millisecond)
	} else {
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(makeElectionTimeout())
	}
}

func (rf *Raft) beFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.switchLeader(false)
	rf.persist()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.leader
}

func (rf *Raft) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
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
	rf.persister.Save(rf.serialize(), rf.persister.snapshot)
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.Save(rf.serialize(), snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("decode error\n")
	}
	rf.lastApplied = rf.prevLogIndex()
	rf.commitIndex = rf.prevLogIndex()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// outdated leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// we are outdated, become follower
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	assert(args.Term == rf.currentTerm, "term mismatch")
	assert(!rf.leader, "leader should not receive InstallSnapshot")
	rf.electionTimer.Reset(makeElectionTimeout())
	// log.Println("[Raft", rf.me, "] [install] from leader term", args.Term, "with logs[:", args.LastIncludedIndex+1, "] my logs[", rf.firstLogIndex(), ":", rf.nextLogIndex(), "]")
	reply.Term = rf.currentTerm

	// assert(args.LastIncludedIndex > rf.commitIndex, "snapshot index <= commitIndex")

	// truncate logs
	dummy := []LogEntry{{Term: args.LastIncludedTerm, Command: args.LastIncludedIndex}}
	rf.log = append(dummy, rf.getLogFrom(args.LastIncludedIndex+1)...)

	// apply snapshot
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.persistWithSnapshot(args.Data)

	// update commitIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.prevLogIndex() {
		return
	}

	// log.Println("[Raft", rf.me, "] [snapshot] log[", rf.firstLogIndex(), ":", index+1, "]")
	rf.log = rf.getLogFrom(index)
	rf.log[0].Command = index

	rf.persistWithSnapshot(snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) isPeerUptoDate(args *RequestVoteArgs) bool {
	lastLogTerm := rf.lastLogTerm()

	// client from future, we are outdated
	if args.LastLogTerm > lastLogTerm {
		return true
	}

	// client in the same term, check log index
	if args.LastLogTerm == lastLogTerm {
		return args.LastLogIndex >= rf.lastLogIndex()
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// outdated client
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// we are outdated, become follower
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	assert(args.Term == rf.currentTerm, "term mismatch")
	reply.Term = rf.currentTerm

	// check if we can vote for the candidate
	// 1. candidate's log is at least as up-to-date as receiver's log
	// 2. receiver hasn't voted for another candidate
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isPeerUptoDate(args) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	// log.Println("[Raft", rf.me, "] [vote]", args.CandidateId, "in term", rf.currentTerm, ":", reply.VoteGranted)
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// outdated leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// we are outdated, become follower
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	assert(args.Term == rf.currentTerm, "term mismatch")
	assert(!rf.leader, "leader should not receive AppendEntries")
	rf.electionTimer.Reset(makeElectionTimeout())
	// log.Println("[Raft", rf.me, "] [ping] from leader term", rf.currentTerm, "with log[", args.PrevLogIndex+1, ":", args.PrevLogIndex+1+len(args.Entries), "]")
	reply.Term = rf.currentTerm

	// conflict because we don't have the prev log entry
	// ===============[....we have....]
	// ========[....we got....]
	// assert(args.PrevLogIndex >= rf.prevLogIndex(), "args.PrevLogIndex < rf.prevLogIndex")
	if args.PrevLogIndex < rf.prevLogIndex() {
		reply.Success = false
		// give us later log index
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.nextLogIndex()
		return

		// 	// truncate logs
		// 	dropCount := rf.prevLogIndex() - args.PrevLogIndex
		// 	args.Entries = args.Entries[dropCount:]
		// 	args.PrevLogIndex = rf.prevLogIndex()
		// 	args.PrevLogTerm = rf.prevLogTerm()
	}

	// conflict because of missing log entries
	// =====[....we have....]
	// ==========================[....we got....]
	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.nextLogIndex()
		return
	}

	// conflict because log entry's term doesn't match
	// ===============[x....we have....]
	// ===============[y....we got....]
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		// Find the first index that has the same term as provided
		reply.ConflictIndex = rf.prevLogIndex()
		for idx := 1; idx < len(rf.log); idx++ {
			if rf.log[idx].Term == reply.ConflictTerm {
				reply.ConflictIndex = rf.getLogIndex(idx)
				break
			}
		}
		return
	}

	reply.Success = true

	// Append log entries if necessary
	if len(args.Entries) > 0 {
		logs := rf.log[:args.PrevLogIndex+1-rf.prevLogIndex()]
		rf.log = append(logs, args.Entries...)
		defer rf.persist()
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.commitCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.leader {
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	// log.Println("[Raft", rf.me, "] [start]", rf.lastLogIndex())
	rf.persist()
	rf.doReplicate(false)
	return rf.lastLogIndex(), rf.currentTerm, true
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

func (rf *Raft) doElection() {
	// no leader heartbeats received
	var args RequestVoteArgs
	// log.Println("[Raft", rf.me, "] [election] for term", rf.currentTerm+1)
	{
		rf.mu.Lock()

		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
		args = RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
		}

		rf.mu.Unlock()
	}

	votes := atomic.Int32{}
	votes.Add(1)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(i, &args, &reply) {
				// failed to send request
				return
			}

			if reply.VoteGranted {
				// log.Println("[Raft", rf.me, "] [received] vote from", i)
				votes := int(votes.Add(1))

				rf.mu.Lock()
				// check if we have majority votes
				// and we are still in the same term
				// and we are not already become the leader
				if votes > len(rf.peers)/2 &&
					rf.currentTerm == args.Term && !rf.leader {
					// we are the leader
					// log.Println("[Raft", rf.me, "] [leader] for term", rf.currentTerm)
					rf.switchLeader(true)
					rf.doReplicate(true)
				}
				rf.mu.Unlock()
			} else {
				// check if we are outdated
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) doReplicate(once bool) {
	// replicate logs to all followers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if once {
			go rf.replicateTo(i)
		} else {
			rf.replicateCond[i].Signal()
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		select {
		case <-rf.electionTimer.C:
			rf.electionTimer.Reset(makeElectionTimeout())
			rf.doElection()

		case <-rf.heartbeatTimer.C:
			rf.heartbeatTimer.Reset(HeartbeatIntervalMs * time.Millisecond)
			rf.doReplicate(true)
		}
	}
}

func (rf *Raft) needReplicateTo(i int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.leader && rf.matchIndex[i] < rf.lastLogIndex()
}

func (rf *Raft) replicateByIS(i int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	// rf.debug("%d needed log has been archived, send IS...\n", i)
	if rf.sendInstallSnapshot(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm == args.Term && rf.leader {
			if reply.Term > rf.currentTerm {
				rf.beFollower(reply.Term)
				return
			}

			rf.matchIndex[i] = args.LastIncludedIndex
			rf.nextIndex[i] = args.LastIncludedIndex + 1
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	commitIndex := topK(rf.matchIndex, (len(rf.peers)-1)/2)
	if commitIndex > rf.commitIndex {
		// Raft Paper's Figure 8
		// Raft never commits log entries from previous terms by counting
		// replicas. Only log entries from the leader’s current term are
		// committed by counting replicas; once an entry from the current term
		// has been committed in this way, then all prior entries are
		// committed indirectly because of the Log Matching Property.
		if rf.getLogTerm(commitIndex) == rf.currentTerm {
			rf.commitIndex = commitIndex
			// log.Println("[Raft", rf.me, "] [update] commit index to", commitIndex)
			rf.commitCond.Signal()
		}
	}
}

func (rf *Raft) replicateByAE(i int, args *AppendEntriesArgs, nextIndex int) {
	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(i, args, reply) {
		// net failure
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.updateCommitIndex()
	} else {
		// log.Println("[Raft", rf.me, "] [replicateAE rejected] by", i, "because", reply)
		// Reject because of outdateness
		if reply.Term > rf.currentTerm {
			rf.beFollower(reply.Term)
		} else if reply.Term == rf.currentTerm {
			// Reject because of unsat logs
			rf.nextIndex[i] = reply.ConflictIndex
			if reply.ConflictTerm != -1 {
				firstLogIndex := rf.firstLogIndex()
				for j := args.PrevLogIndex; j >= firstLogIndex; j-- {
					if rf.getLog(j).Term == reply.ConflictTerm {
						rf.nextIndex[i] = j + 1
						break
					}
				}
			}
		}
	}
}

func (rf *Raft) replicateTo(i int) {
	rf.mu.RLock()
	if !rf.leader {
		rf.mu.RUnlock()
		return
	}
	nextIndex := rf.nextIndex[i]
	if nextIndex < rf.firstLogIndex() {
		assert(len(rf.persister.ReadSnapshot()) > 0, "snapshot is empty")
		// log.Println("[Raft", rf.me, "] [replicateIS] to", i, "nextIndex", nextIndex)
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.prevLogIndex(),
			LastIncludedTerm:  rf.prevLogTerm(),
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.RUnlock()
		rf.replicateByIS(i, args)
	} else {
		// log.Println("[Raft", rf.me, "] [replicateAE] to", i, "nextIndex", nextIndex)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getLogTerm(nextIndex - 1),
			Entries:      rf.copyLogFrom(nextIndex),
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.RUnlock()
		rf.replicateByAE(i, args, nextIndex)
	}
}

// Replicator is a goroutine that replicates logs to a peer by batch.
func (rf *Raft) replicator(i int) {
	rf.replicateCond[i].L.Lock()
	defer rf.replicateCond[i].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicateTo(i) {
			rf.replicateCond[i].Wait()
		}
		// TODO: unlock before replicate?
		rf.replicateTo(i)
	}
}

func (rf *Raft) commiter() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.commitCond.Wait()
		}
		assert(rf.commitIndex > rf.lastApplied, "commitIndex <= lastApplied")
		// log.Println("[Raft", rf.me, "] [commit] log[", rf.lastApplied+1, ":", rf.commitIndex+1, "]")
		for i, entry := range rf.getLogSlice(rf.lastApplied+1, rf.commitIndex+1) {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: rf.lastApplied + 1 + i}
		}
		rf.lastApplied = rf.commitIndex
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = make(chan ApplyMsg, 100)

	// Your initialization code here (3A, 3B, 3C).
	rf.leader = false
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.replicateCond = make([]*sync.Cond, len(peers))
	rf.electionTimer = time.NewTimer(makeElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartbeatIntervalMs * time.Millisecond)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // index 0 is dummy
	// log[0].Term is the term of the prev log entry
	// log[0].Command is index of the prev log entry
	rf.log[0] = LogEntry{Term: 0, Command: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.replicateCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commiter()
	go func() {
		// applier
		for msg := range rf.applyCh {
			applyCh <- msg
		}
	}()

	return rf
}
