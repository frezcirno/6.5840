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
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const HeartbeatIntervalMs = 100
const ElectionMinTimeoutMs = 500
const ElectionMaxTimeoutMs = 800

func makeElectionTimeout() time.Duration {
	return time.Duration(ElectionMinTimeoutMs+rand.Intn(ElectionMaxTimeoutMs-ElectionMinTimeoutMs)) * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
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
		panic(message)
	}
}

func assert_eq(a, b int, message string) {
	if a != b {
		msg := message + ": " + strconv.Itoa(a) + " != " + strconv.Itoa(b)
		panic(msg)
	}
}

func assert_ne(a, b int, message string) {
	if a == b {
		msg := message + ": " + strconv.Itoa(a) + " == " + strconv.Itoa(b)
		panic(msg)
	}
}

func assert_lt(a, b int, message string) {
	if !(a < b) {
		msg := message + ": " + strconv.Itoa(a) + " >= " + strconv.Itoa(b)
		panic(msg)
	}
}

func assert_le(a, b int, message string) {
	if !(a <= b) {
		msg := message + ": " + strconv.Itoa(a) + " > " + strconv.Itoa(b)
		panic(msg)
	}
}

func assert_gt(a, b int, message string) {
	if !(a > b) {
		msg := message + ": " + strconv.Itoa(a) + " <= " + strconv.Itoa(b)
		panic(msg)
	}
}

func assert_ge(a, b int, message string) {
	if !(a >= b) {
		msg := message + ": " + strconv.Itoa(a) + " < " + strconv.Itoa(b)
		panic(msg)
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
	CommandTerm  int
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
	applyCond      *sync.Cond
	replicateCond  []*sync.Cond

	currentTerm int
	votedFor    int
	// log[0] is dummy, log[0].Command is the logStartIndex
	// log[i] is the (logStartIndex+i)-th log entry, numbered from 1
	log []LogEntry

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	applying int32
	// If true, use asynchronous InstallSnapshot
	// Asynchronous InstallSnapshot is faster but cannot pass lab 3D test
	// Lab 3D test requires the service to switch to the snapshot immediately
	useAsyncInstallSnapshot bool

	tag string // for debug
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

// Get log entries from [from:], from is the global log index
func (rf *Raft) getLogFrom(from int) []LogEntry {
	assert_le(rf.firstLogIndex(), from, "index out of range")
	if from >= rf.nextLogIndex() {
		return []LogEntry{}
	}
	return rf.log[from-rf.prevLogIndex():]
}

func (rf *Raft) getLogSlice(from int, to int) []LogEntry {
	assert_le(rf.firstLogIndex(), from, "index out of range")
	assert_le(to, rf.nextLogIndex(), "index out of range")
	return rf.log[from-rf.prevLogIndex() : to-rf.prevLogIndex()]
}

func (rf *Raft) getLogIndex(index int) int {
	// local log index -> global log index
	assert_le(1, index, "index out of range")
	assert_le(index, len(rf.log), "index out of range")
	return rf.prevLogIndex() + index
}

func (rf *Raft) getLogTerm(index int) int {
	// Note: allow get term of dummy log entry
	assert_le(rf.prevLogIndex(), index, "index out of range")
	assert_le(index, rf.lastLogIndex(), "index out of range")
	return rf.log[index-rf.prevLogIndex()].Term
}

func (rf *Raft) hasLog(index int) bool {
	return rf.firstLogIndex() <= index && index <= rf.lastLogIndex()
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
	e.Encode(rf.tag)
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
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var tag string
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&tag) != nil {
		panic("decode error\n")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.tag = tag
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

	// outdated leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// we are outdated, become follower
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	assert_eq(args.Term, rf.currentTerm, "term mismatch")
	assert(!rf.leader, "leader should not receive InstallSnapshot")
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(makeElectionTimeout())

	// outdated snapshot
	if args.LastIncludedIndex <= rf.prevLogIndex() || args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	assert_gt(args.LastIncludedIndex, rf.prevLogIndex(), "old snapshot")
	assert_gt(args.LastIncludedIndex, rf.commitIndex, "outdated snapshot")

	if !rf.useAsyncInstallSnapshot {
		rf.doInstallSnapshot(args.LastIncludedTerm, args.LastIncludedIndex, args.Data)
		for atomic.LoadInt32(&rf.applying) > 0 {
			// wait for old logs to be applied
			time.Sleep(1 * time.Millisecond)
		}
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
		go func() {
			// Notify the service that we are going to switch to a snapshot.
			// When the service is ready, it should call `TryInstallSnapshot`
			// and switch to the snapshot.
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}()
	}
}

// Try to switch to the snapshot, return true if successful
func (rf *Raft) TryInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	if !rf.useAsyncInstallSnapshot {
		return true
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// outdated snapshot
	if lastIncludedIndex <= rf.prevLogIndex() || lastIncludedIndex <= rf.commitIndex {
		return false
	}

	return rf.doInstallSnapshot(lastIncludedTerm, lastIncludedIndex, snapshot)
}

func (rf *Raft) doInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// truncate logs
	dummy := []LogEntry{{Term: lastIncludedTerm, Command: lastIncludedIndex}}
	rf.log = append(dummy, rf.getLogFrom(lastIncludedIndex+1)...)

	// update commitIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	assert_le(rf.commitIndex, rf.lastLogIndex(), "commitIndex > lastLogIndex")
	assert_ge(rf.lastApplied, rf.prevLogIndex(), "lastApplied < prevLogIndex")

	rf.persistWithSnapshot(snapshot)
	return true
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

	// The log entry at index is already included in the snapshot
	if index <= rf.prevLogIndex() {
		return
	}

	// log.Printf("[raft-%s%d] [snapshot] to log[:%d]", rf.tag, rf.me, index+1)
	// truncate logs
	rf.log = rf.getLogFrom(index)
	rf.log[0].Command = index
	assert_le(rf.commitIndex, rf.lastLogIndex(), "commitIndex > lastLogIndex")

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

func (rf *Raft) isPeerUpToDate(args *RequestVoteArgs) bool {
	lastLogTerm := rf.lastLogTerm()

	// client from future, we are outdated
	if args.LastLogTerm > lastLogTerm {
		return true
	}

	// client from past, they are outdated
	if args.LastLogTerm < lastLogTerm {
		return false
	}

	// client in the same term, check log index
	return args.LastLogIndex >= rf.lastLogIndex()
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

	assert_eq(args.Term, rf.currentTerm, "term mismatch")
	reply.Term = rf.currentTerm

	// check if we can vote for the candidate
	// 1. candidate's log is at least as up-to-date as receiver's log
	// 2. receiver hasn't voted for another candidate
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isPeerUpToDate(args) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	// log.Printf("[raft-%s%d] [election vote] for %d in term %d: %v", rf.tag, rf.me, args.CandidateId, rf.currentTerm, reply.VoteGranted)
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
	Entries      []LogEntry // not include the dummy log
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

	assert_eq(args.Term, rf.currentTerm, "term mismatch")
	assert(!rf.leader, "leader should not receive AppendEntries")
	rf.electionTimer.Reset(makeElectionTimeout())
	// log.Printf("[raft-%s%d] [ping] from leader term %d, with log[%d:%d], my log[%d:%d], commitIndex: %d, lastApplied: %d", rf.tag, rf.me, args.Term, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries), rf.firstLogIndex(), rf.nextLogIndex(), rf.commitIndex, rf.lastApplied)
	reply.Term = rf.currentTerm

	// conflict because we don't have the prev log entry
	// ===============[....we have....]
	// ========[....we got....]
	if args.PrevLogIndex < rf.prevLogIndex() {
		reply.Success = false
		// give us later log index
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.nextLogIndex()
		return
	}
	assert(args.PrevLogIndex >= rf.prevLogIndex(), "args.PrevLogIndex < rf.prevLogIndex")

	// cannot match because of missing log entries
	// =====[....we have....]
	// ==========================[....we got....]
	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.nextLogIndex()
		return
	}

	assert_le(args.PrevLogIndex, rf.lastLogIndex(), "args.PrevLogIndex > rf.lastLogIndex")

	// conflict because log entry's term doesn't match
	// ===============[x....we have....]
	// =============[..y....we got...]
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

	assert_eq(args.PrevLogTerm, rf.getLogTerm(args.PrevLogIndex), "term mismatch")
	reply.Success = true

	// Overwrite unmatched log entries
	// If no conflict, just append the new logs if any
	// ===============[.....we have.....]
	// ===============[...we got...]
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if !rf.hasLog(index) || rf.getLogTerm(index) != entry.Term {
			// found conflict, truncate logs
			log := rf.log[:index-rf.prevLogIndex()]
			rf.log = append(log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// update commitIndex
	// sometimes the leader's commitIndex is ahead of the follower's
	newCommitIndex := min(args.LeaderCommit, rf.lastLogIndex())
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	assert_le(rf.commitIndex, rf.lastLogIndex(), "commitIndex > lastLogIndex")
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
	assert_le(rf.commitIndex, rf.lastLogIndex(), "commitIndex > lastLogIndex")
	rf.persist()
	// log.Printf("[raft-%s%d] [start] log[%d] with command %v", rf.tag, rf.me, rf.lastLogIndex(), command)
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
	{
		rf.mu.Lock()

		// log.Printf("[raft-%s%d] [election] for term %d", rf.tag, rf.me, rf.currentTerm+1)
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
				// log.Printf("[raft-%s%d] [election receive vote] from %d", rf.tag, rf.me, i)
				votes := int(votes.Add(1))

				rf.mu.Lock()
				// check if we have majority votes
				// and we are still in the same term
				// and we are not already become the leader
				if votes > len(rf.peers)/2 &&
					rf.currentTerm == args.Term && !rf.leader {
					// we are the leader
					// log.Printf("[raft-%s%d] [election leader] for term %d", rf.tag, rf.me, rf.currentTerm)
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

// replicate logs to all followers
func (rf *Raft) doReplicate(once bool) {
	rf.heartbeatTimer.Reset(HeartbeatIntervalMs * time.Millisecond)
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
	if !rf.sendInstallSnapshot(i, args, reply) {
		// net failure
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// we are still the leader
	if rf.currentTerm != args.Term || !rf.leader {
		return
	}

	// we are outdated
	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}

	rf.matchIndex[i] = args.LastIncludedIndex
	rf.nextIndex[i] = args.LastIncludedIndex + 1
	rf.updateCommitIndex()
}

func (rf *Raft) updateCommitIndex() {
	newCommitIndex := topK(rf.matchIndex, (len(rf.peers)-1)/2)
	if newCommitIndex > rf.commitIndex {
		// Raft Paper's Figure 8
		// Raft never commits log entries from previous terms by counting
		// replicas. Only log entries from the leader’s current term are
		// committed by counting replicas; once an entry from the current term
		// has been committed in this way, then all prior entries are
		// committed indirectly because of the Log Matching Property.
		if rf.getLogTerm(newCommitIndex) == rf.currentTerm {
			rf.commitIndex = newCommitIndex
			assert_le(rf.commitIndex, rf.lastLogIndex(), "commitIndex > lastLogIndex")
			// log.Printf("[raft-%s%d] [update] commit index to %d, lastApplied %d", rf.tag, rf.me, newCommitIndex, rf.lastApplied)
			rf.applyCond.Signal()
			// immediately notify other followers to update their commitIndex
			rf.doReplicate(true)
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

	// we are still the leader
	if rf.currentTerm != args.Term || !rf.leader {
		return
	}

	if reply.Success {
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.updateCommitIndex()
	} else {
		// Reject because of outdateness
		// log.Printf("[raft-%s%d] [replicate rejected] by %d, nextIndex %d, conflictIndex %d, conflictTerm %d", rf.tag, rf.me, i, rf.nextIndex[i], reply.ConflictIndex, reply.ConflictTerm)

		// we are outdated
		if reply.Term > rf.currentTerm {
			rf.beFollower(reply.Term)
			return
		}

		// Reject because of unsat logs
		if reply.Term == rf.currentTerm {
			rf.nextIndex[i] = reply.ConflictIndex
			if reply.ConflictTerm != -1 {
				firstLogIndex := rf.firstLogIndex()
				for j := args.PrevLogIndex; j >= firstLogIndex; j-- {
					if rf.getLogTerm(j) == reply.ConflictTerm {
						rf.nextIndex[i] = j + 1
						break
					}
				}
			}
			// resend immediately
			rf.replicateCond[i].Signal()
		}
	}
}

func copySlice(entries []LogEntry) []LogEntry {
	newEntries := make([]LogEntry, len(entries))
	copy(newEntries, entries)
	return newEntries
}

func (rf *Raft) replicateTo(i int) {
	rf.mu.RLock()
	if !rf.leader {
		rf.mu.RUnlock()
		return
	}

	nextIndex := rf.nextIndex[i]
	if nextIndex < rf.firstLogIndex() {
		assert_gt(len(rf.persister.ReadSnapshot()), 0, "snapshot is empty")
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.prevLogIndex(),
			LastIncludedTerm:  rf.prevLogTerm(),
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.RUnlock()
		// log.Printf("[raft-%s%d] [replicate snapshot] to %d with log[:%d], whose nextIndex %d", rf.tag, rf.me, i, args.LastIncludedIndex+1, nextIndex)
		rf.replicateByIS(i, args)
	} else {
		assert_le(rf.commitIndex, rf.lastLogIndex(), "commitIndex > lastLogIndex")
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.getLogTerm(nextIndex - 1),
			// copy to avoid data race
			Entries:      copySlice(rf.getLogFrom(nextIndex)), // not include the dummy log
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.RUnlock()
		// log.Printf("[raft-%s%d] [replicate logs] to %d with log[%d:%d], whose nextIndex %d", rf.tag, rf.me, i, args.PrevLogIndex+1, args.PrevLogIndex+1+len(args.Entries), nextIndex)
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

// Asynchronously apply logs to the state machine for performance
// Notice: the applier may apply logs OUT-OF-ORDER!
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			// apply logs
			assert_gt(rf.commitIndex, rf.lastApplied, "commitIndex <= lastApplied")
			assert_le(rf.commitIndex, rf.lastLogIndex(), "commitIndex > lastLogIndex")
			assert_ge(rf.lastApplied, rf.prevLogIndex(), "lastApplied < prevLogIndex")

			startIndex := rf.lastApplied + 1
			// log.Printf("[raft-%s%d] [apply async] log[%d:%d]", rf.tag, rf.me, startIndex, rf.commitIndex+1)
			entries := append([]LogEntry{}, rf.getLogSlice(startIndex, rf.commitIndex+1)...)
			rf_commitIndex := rf.commitIndex
			rf.mu.Unlock()
			atomic.StoreInt32(&rf.applying, 1)
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandTerm:  entry.Term,
					CommandIndex: startIndex + i,
				}
			}
			atomic.StoreInt32(&rf.applying, 0)
			rf.mu.Lock()
			rf.lastApplied = Max(rf.lastApplied, rf_commitIndex)
		} else {
			// wait
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) SetTag(tag string) {
	rf.tag = tag
}

func (rf *Raft) SetAsyncInstallSnapshot(enable bool) {
	rf.useAsyncInstallSnapshot = enable
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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.leader = false
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicateCond = make([]*sync.Cond, len(peers))
	rf.electionTimer = time.NewTimer(makeElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartbeatIntervalMs * time.Millisecond)
	rf.heartbeatTimer.Stop()

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

	rf.applying = 0
	rf.useAsyncInstallSnapshot = false
	rf.tag = ""

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// log.Printf("[raft-%s%d] [init] with log[%d:%d], leader %v, commitIndex %d, lastApplied %d", rf.tag, rf.me, rf.firstLogIndex(), rf.nextLogIndex(), rf.leader, rf.commitIndex, rf.lastApplied)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.replicateCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
