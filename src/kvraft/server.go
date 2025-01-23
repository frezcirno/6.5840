package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false
const ExecuteTimeout = 500 * time.Millisecond

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int64
	Op       OpType
	Key      string
	Value    string
}

type Result struct {
	Err   Err
	Value string
}

type Last struct {
	Seq    int64
	Result *Result
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store  map[string]string
	last   map[int64]Last
	notify map[int]chan *Result

	persister *raft.Persister

	// As the underlying raft implementation may apply snapshot asynchronously,
	// we need to keep track of the last applied log index to avoid re-applying
	// the same log after a snapshot is applied.
	lastExecuted int
}

func (kv *KVServer) isDup(clientId, seq int64) bool {
	last, ok := kv.last[clientId]
	if !ok {
		return false
	}
	assert(seq >= last.Seq, "seq < last.seq")
	return seq == last.Seq
}

func (kv *KVServer) getNotify(index int) chan *Result {
	if _, ok := kv.notify[index]; !ok {
		kv.notify[index] = make(chan *Result, 1)
	}
	return kv.notify[index]
}

func (kv *KVServer) purpose(op *Op) (value string, err Err) {
	logIndex, _, leader := kv.rf.Start(*op)
	if !leader {
		err = ErrWrongLeader
		return
	}

	// log.Printf("[kvraft-%d] [start] %d {%d %d %v %s %s}", kv.me, logIndex, op.ClientId, op.Seq, op.Op, op.Key, op.Value)

	kv.mu.Lock()
	notify := kv.getNotify(logIndex)
	kv.mu.Unlock()

	// wait for the result
	select {
	case r := <-notify:
		value = r.Value
		err = r.Err
	case <-time.After(ExecuteTimeout):
		// timeout, ask client to poll again later
		err = ErrTimeout
	}

	// clean up
	go func() {
		kv.mu.Lock()
		delete(kv.notify, logIndex)
		kv.mu.Unlock()
	}()

	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Value, reply.Err = kv.purpose(&Op{
		Key:      args.Key,
		Op:       OpGet,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.RLock()
	if kv.isDup(args.ClientId, args.Seq) {
		reply.Err = kv.last[args.ClientId].Result.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	_, reply.Err = kv.purpose(&Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       OpPut,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.RLock()
	if kv.isDup(args.ClientId, args.Seq) {
		reply.Err = kv.last[args.ClientId].Result.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	_, reply.Err = kv.purpose(&Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       OpAppend,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) execute(op *Op) *Result {
	result := &Result{}
	switch op.Op {
	case OpGet:
		result.Value = kv.store[op.Key]
		result.Err = OK
	case OpPut:
		if kv.isDup(op.ClientId, op.Seq) {
			return kv.last[op.ClientId].Result
		}

		kv.store[op.Key] = op.Value
		result.Err = OK

		kv.last[op.ClientId] = Last{Seq: op.Seq, Result: result}
	case OpAppend:
		if kv.isDup(op.ClientId, op.Seq) {
			return kv.last[op.ClientId].Result
		}

		kv.store[op.Key] += op.Value
		result.Err = OK

		kv.last[op.ClientId] = Last{Seq: op.Seq, Result: result}
	default:
		panic("Unknown Operation")
	}
	// log.Printf("[kvraft-%d] [execute] {%d %d %v %s %s}", kv.me, op.ClientId, op.Seq, op.Op, op.Key, op.Value)
	return result
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.store) != nil ||
		e.Encode(kv.last) != nil {
		log.Panicln("Encode failed")
	}
	return w.Bytes()
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var store map[string]string
	var last map[int64]Last
	if d.Decode(&store) != nil ||
		d.Decode(&last) != nil {
		log.Panicln("Decode failed")
	}
	kv.store = store
	kv.last = last
}

func (kv *KVServer) executor() {
	for !kv.killed() {
		msg := <-kv.applyCh
		assert(msg.CommandValid || msg.SnapshotValid, "invalid message")

		if msg.CommandValid {
			op := msg.Command.(Op)
			kv.mu.Lock()

			// skip if the log applied is already executed
			if msg.CommandIndex <= kv.lastExecuted {
				kv.mu.Unlock()
				continue
			}
			assert(msg.CommandIndex == kv.lastExecuted+1 || kv.lastExecuted == 0, "unexpected command index")
			kv.lastExecuted = msg.CommandIndex

			result := kv.execute(&op)

			// only notify related channel for currentTerm's log when node is leader
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader && msg.CommandTerm == currentTerm {
				ch := kv.getNotify(msg.CommandIndex)
				ch <- result
			}

			// snapshot if log grows too big
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.makeSnapshot()
				kv.mu.Unlock()
				// async submit snapshot to raft
				go func(commandIndex int) {
					kv.rf.Snapshot(commandIndex, snapshot)
				}(msg.CommandIndex)
				continue
			}
			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			if kv.rf.TryInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.restoreSnapshot(msg.Snapshot)
				kv.lastExecuted = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}

	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.last = make(map[int64]Last)
	kv.notify = make(map[int]chan *Result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastExecuted = 0

	kv.restoreSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.executor()

	return kv
}
