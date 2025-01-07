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

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func assert(cond bool, msg string) {
	if Debug {
		if !cond {
			log.Panicln(msg)
		}
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string
	ClientId int64
	Seq      int64
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
}

func (kv *KVServer) isDup(clientId, seq int64) bool {
	last, ok := kv.last[clientId]
	if !ok {
		return false
	}
	assert(seq >= last.Seq, "seq < last.seq")
	return seq == last.Seq
}

func (kv *KVServer) getNotify(logIndex int) chan *Result {
	if _, ok := kv.notify[logIndex]; !ok {
		kv.notify[logIndex] = make(chan *Result, 1)
	}
	return kv.notify[logIndex]
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// log.Println("KV", kv.me, "Get", args.ClientId, args.Seq)

	kv.mu.RLock()
	if kv.isDup(args.ClientId, args.Seq) {
		lastResult := kv.last[args.ClientId].Result
		reply.Value = lastResult.Value
		reply.Err = lastResult.Err
		kv.mu.RUnlock()
		// log.Println("KV", kv.me, "Get", args.ClientId, args.Seq, "is duplicated")
		return
	}
	kv.mu.RUnlock()

	logIndex, _, leader := kv.rf.Start(Op{
		Key:      args.Key,
		Op:       "Get",
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !leader {
		reply.Err = ErrWrongLeader
		// log.Println("KV", kv.me, "Get", args.ClientId, args.Seq, "fails: not leader")
		return
	}

	kv.mu.Lock()
	notify := kv.getNotify(logIndex)
	kv.mu.Unlock()

	// wait for the result
	select {
	case r := <-notify:
		reply.Value = r.Value
		reply.Err = r.Err
	case <-time.After(ExecuteTimeout):
		// timeout, ask client to poll again later
		reply.Err = ErrTimeout
	}

	// clean up
	go func() {
		kv.mu.Lock()
		delete(kv.notify, logIndex)
		kv.mu.Unlock()
	}()

	// log.Println("KV", kv.me, "Get", args.ClientId, args.Seq, "finished", reply.Err)
}

func (kv *KVServer) putAppend(op string, args *PutAppendArgs, reply *PutAppendReply) {
	// log.Println("KV", kv.me, op, args.ClientId, args.Seq)

	kv.mu.RLock()
	if kv.isDup(args.ClientId, args.Seq) {
		lastResult := kv.last[args.ClientId].Result
		reply.Err = lastResult.Err
		kv.mu.RUnlock()
		// log.Println("KV", kv.me, op, args.ClientId, args.Seq, "is duplicated")
		return
	}
	kv.mu.RUnlock()

	logIndex, _, leader := kv.rf.Start(Op{
		Key:      args.Key,
		Value:    args.Value,
		Op:       op,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	if !leader {
		reply.Err = ErrWrongLeader
		// log.Println("KV", kv.me, op, args.ClientId, args.Seq, "fails: not leader")
		return
	}

	kv.mu.Lock()
	notify := kv.getNotify(logIndex)
	kv.mu.Unlock()

	// wait for the result
	select {
	case r := <-notify:
		reply.Err = r.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	// clean up
	go func() {
		kv.mu.Lock()
		delete(kv.notify, logIndex)
		kv.mu.Unlock()
	}()

	// log.Println("KV", kv.me, op, args.ClientId, args.Seq, "finished", reply.Err)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend("Put", args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend("Append", args, reply)
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

func (kv *KVServer) execute(op Op) *Result {
	var result Result
	switch op.Op {
	case "Get":
		result.Value = kv.store[op.Key]
		result.Err = OK
	case "Put":
		kv.store[op.Key] = op.Value
		result.Err = OK
	case "Append":
		kv.store[op.Key] += op.Value
		result.Err = OK
	default:
		panic("Unknown Operation")
	}
	return &result
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.last)
	return w.Bytes()
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.store) != nil ||
		d.Decode(&kv.last) != nil {
		log.Panicln("restoreSnapshot failed")
	}
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}

		assert(msg.CommandValid || msg.SnapshotValid, "invalid message")
		kv.mu.Lock()

		if msg.SnapshotValid {
			// log.Println("KV", kv.me, "Message", msg, "restoring", msg.SnapshotIndex)
			kv.restoreSnapshot(msg.Snapshot)
			// log.Println("KV", kv.me, "Message", msg, "restored")
		} else {
			// log.Println("KV", kv.me, "Message", msg)
			op := msg.Command.(Op)
			if !kv.isDup(op.ClientId, op.Seq) {
				result := kv.execute(op)
				kv.last[op.ClientId] = Last{Seq: op.Seq, Result: result}
			}

			// only notify related channel for currentTerm's log when node is leader
			currentTerm, isLeader := kv.rf.GetState()
			if isLeader && msg.CommandTerm == currentTerm {
				ch := kv.getNotify(msg.CommandIndex)
				ch <- kv.last[op.ClientId].Result
			}

			// snapshot if log grows too big
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				// log.Println("KV", kv.me, "Message", msg, "snapshotting")
				kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
			}
			// log.Println("KV", kv.me, "Message", msg, "applied")
		}

		kv.mu.Unlock()
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.restoreSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applier()

	return kv
}
