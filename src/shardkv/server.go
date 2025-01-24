package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const ExecuteTimeout = 500 * time.Millisecond

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

type OpType uint8

const (
	Get OpType = iota
	Put
	Append
	Config
	InstallShards
	RecycleShards
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op OpType
	// For Get, Put, Append
	ClientId int64
	Seq      int64
	Key      string
	Value    string
	// For Config
	Config shardctrler.Config
	// For InstallShards
	ConfigNum  int
	ShardStore map[int]map[string]string
	Last       map[int64]Last
	// For RecycleShards
	OkShards []int
}

type InstallShardsArgs struct {
	Seq        int64
	ConfigNum  int
	ShardStore map[int]map[string]string
	Last       map[int64]Last
}

type InstallShardsReply struct {
	Err   Err
	Value string
}

type Result struct {
	Err   Err
	Value string
}

type Last struct {
	Seq    int64
	Result *Result
}

type ShardState uint8

const (
	Default ShardState = iota
	Loading
	Recycling
)

type Shard struct {
	State ShardState
	Store map[string]string
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister    *raft.Persister
	lastExecuted int

	dead   int32 // set by Kill()
	shards []Shard
	last   map[int64]Last
	notify map[int]chan *Result
	cfg    shardctrler.Config
	sc     *shardctrler.Clerk
}

func (kv *ShardKV) isDup(clientId, seq int64) bool {
	last, ok := kv.last[clientId]
	if !ok {
		return false
	}
	assert(seq >= last.Seq, "seq < last.seq")
	return seq == last.Seq
}

func (kv *ShardKV) getNotify(index int) chan *Result {
	if _, ok := kv.notify[index]; !ok {
		kv.notify[index] = make(chan *Result, 1)
	}
	return kv.notify[index]
}

func (kv *ShardKV) purpose(op *Op) (value string, err Err) {
	logIndex, _, leader := kv.rf.Start(*op)
	if !leader {
		return "", ErrWrongLeader
	}

	log.Printf("ShardKV-%d%d [purpose] %v\n", kv.gid, kv.me, *op)

	kv.mu.Lock()
	notify := kv.getNotify(logIndex)
	kv.mu.Unlock()

	// wait for the result
	select {
	case r := <-notify:
		value, err = r.Value, r.Err
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

func (kv *ShardKV) InstallShards(args *InstallShardsArgs, reply *InstallShardsReply) {
	kv.mu.RLock()
	if args.ConfigNum < kv.cfg.Num {
		reply.Err = ErrOldConfig
		kv.mu.RUnlock()
		return
	}
	if args.ConfigNum > kv.cfg.Num {
		reply.Err = ErrBusy
		kv.mu.RUnlock()
		return
	}
	assert(args.ConfigNum == kv.cfg.Num, "wrong config number")
	kv.mu.RUnlock()

	reply.Value, reply.Err = kv.purpose(&Op{
		Op:         InstallShards,
		Seq:        args.Seq,
		ConfigNum:  args.ConfigNum,
		ShardStore: args.ShardStore,
		Last:       args.Last,
	})
}

func (kv *ShardKV) getShard(key string) (*Shard, Err) {
	shard := key2shard(key)
	if kv.cfg.Shards[shard] != kv.gid {
		return nil, ErrWrongGroup
	}
	sd := &kv.shards[shard]
	if sd.State == Recycling {
		return nil, ErrWrongGroup
	}
	if sd.State == Loading {
		return nil, ErrBusy
	}
	return sd, OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.RLock()
	if _, err := kv.getShard(args.Key); err != OK {
		reply.Err = err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	reply.Value, reply.Err = kv.purpose(&Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       Get,
		Key:      args.Key,
	})
}

func opType(s string) OpType {
	switch s {
	case "Put":
		return Put
	case "Append":
		return Append
	default:
		panic("Unknown Operation")
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.RLock()
	if _, err := kv.getShard(args.Key); err != OK {
		reply.Err = err
		kv.mu.RUnlock()
		return
	}

	if kv.isDup(args.ClientId, args.Seq) {
		reply.Err = kv.last[args.ClientId].Result.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	_, reply.Err = kv.purpose(&Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       opType(args.Op),
		Key:      args.Key,
		Value:    args.Value,
	})
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) readyForConfig() bool {
	for _, sd := range kv.shards {
		if sd.State != Default {
			return false
		}
	}
	return true
}

func (kv *ShardKV) configListener() {
	for !kv.killed() {
		kv.mu.RLock()
		allShardsReady := kv.readyForConfig()
		kvCfgNum := kv.cfg.Num
		kv.mu.RUnlock()

		// propose new config if:
		// 1. all shards are ready for next config
		if allShardsReady {
			cfg := kv.sc.Query(kvCfgNum + 1)
			if cfg.Num != kvCfgNum {
				assert(cfg.Num == kvCfgNum+1, "wrong config number")
				kv.purpose(&Op{
					Op:     Config,
					Config: cfg,
				})
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) transferShards(args *InstallShardsArgs, dst []string) {
	for _, server := range dst {
		// log.Printf("ShardKV-%d%d [sendInstallShards -> %d%d] args: %v\n", kv.gid, kv.me, args.Target, i, args)
		srv := kv.make_end(server)

		var reply InstallShardsReply
		ok := srv.Call("ShardKV.InstallShards", args, &reply)
		if !ok {
			// the server is down
			continue
		}

		// log.Printf("ShardKV-%d%d [sendInstallShards <- %d%d] err: %v\n", kv.gid, kv.me, args.Target, i, reply.Err)

		if reply.Err == ErrWrongGroup {
			// fatal error, no need to retry
			return
		}

		// transfer success, or we are outdated (our shard has been transferred to peers)
		if reply.Err == OK || reply.Err == ErrOldConfig {
			ok_shards := make([]int, 0, len(args.ShardStore))
			for shard := range args.ShardStore {
				ok_shards = append(ok_shards, shard)
			}
			kv.purpose(&Op{
				Op:       RecycleShards,
				OkShards: ok_shards,
			})
			return
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *ShardKV) shardTransmitter() {
	for !kv.killed() {

		kv.mu.RLock()
		if _, ld := kv.rf.GetState(); !ld {
			kv.mu.RUnlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		shards2send := make(map[int][]int)
		for shard, sd := range kv.shards {
			if sd.State == Recycling {
				target := kv.cfg.Shards[shard]
				shards2send[target] = append(shards2send[target], shard)
			}
		}

		operationInProgess := sync.WaitGroup{}
		for target, shards := range shards2send {
			operationInProgess.Add(1)
			shard_store := make(map[int]map[string]string)
			for _, shard := range shards {
				shard_store[shard] = make(map[string]string)
				for key, value := range kv.shards[shard].Store {
					shard_store[shard][key] = value
				}
			}
			kv_last := make(map[int64]Last)
			for clientId, last := range kv.last {
				kv_last[clientId] = last
			}
			args := &InstallShardsArgs{
				ConfigNum:  kv.cfg.Num,
				ShardStore: shard_store,
				Last:       kv_last,
			}
			dst := kv.cfg.Groups[target]
			go func() {
				kv.transferShards(args, dst)
				operationInProgess.Done()
			}()
		}
		kv.mu.RUnlock()
		operationInProgess.Wait()

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) updateShardsState(newCfg *shardctrler.Config) {
	for shard := 0; shard < shardctrler.NShards; shard++ {
		newGid := newCfg.Shards[shard]
		if kv.cfg.Shards[shard] == kv.gid && newGid != kv.gid {
			// shard moved from our to others
			kv.shards[shard].State = Recycling
		} else if kv.cfg.Shards[shard] != kv.gid && newGid == kv.gid && kv.cfg.Num != 0 {
			// shard moved from others to us
			kv.shards[shard].State = Loading
		}
	}
}

func (kv *ShardKV) execute(op *Op) *Result {
	result := &Result{Err: OK}
	switch op.Op {
	case Get:
		// Check state first
		shard, err := kv.getShard(op.Key)
		if err != OK {
			result.Err = err
			break
		}
		result.Value = shard.Store[op.Key]
	case Put:
		fallthrough
	case Append:
		// Check state first
		shard, err := kv.getShard(op.Key)
		if err != OK {
			result.Err = err
			break
		}

		if kv.isDup(op.ClientId, op.Seq) {
			return kv.last[op.ClientId].Result
		}
		kv.last[op.ClientId] = Last{Seq: op.Seq, Result: result}

		if op.Op == Put {
			shard.Store[op.Key] = op.Value
		} else {
			shard.Store[op.Key] += op.Value
		}
	case Config:
		if op.Config.Num <= kv.cfg.Num {
			result.Err = ErrOldConfig
			break
		}
		assert(kv.cfg.Num+1 == op.Config.Num, "wrong config number")
		assert(kv.readyForConfig(), "not ready for new config")

		kv.updateShardsState(&op.Config)
		kv.cfg = op.Config
	case InstallShards:
		if op.ConfigNum < kv.cfg.Num {
			result.Err = ErrOldConfig
			break
		}
		if op.ConfigNum > kv.cfg.Num {
			result.Err = ErrBusy
			break
		}

		// install keys
		for shard, sd := range op.ShardStore {
			if kv.shards[shard].State == Loading {
				// clear original and install
				kv.shards[shard].Store = make(map[string]string)
				for key, value := range sd {
					kv.shards[shard].Store[key] = value
				}
				kv.shards[shard].State = Default
			}
		}

		// install latest last
		for clientId, install := range op.Last {
			if _, ok := kv.last[clientId]; !ok || kv.last[clientId].Seq < install.Seq {
				kv.last[clientId] = install
			}
		}
	case RecycleShards:
		for _, shard := range op.OkShards {
			if kv.shards[shard].State == Recycling {
				kv.shards[shard].Store = make(map[string]string)
				kv.shards[shard].State = Default
			}
		}
	default:
		panic("Unknown Operation")
	}
	log.Printf("ShardKV-%d%d [execute] %v, op: %v\n", kv.gid, kv.me, result.Err, *op)
	return result
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(&kv.shards) != nil ||
		e.Encode(&kv.last) != nil ||
		e.Encode(&kv.cfg) != nil {
		log.Panicln("Encode failed")
	}
	return w.Bytes()
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shard_store []Shard
	var last map[int64]Last
	var cfg shardctrler.Config
	if d.Decode(&shard_store) != nil ||
		d.Decode(&last) != nil ||
		d.Decode(&cfg) != nil {
		log.Panicln("Decode failed")
	}
	kv.shards = shard_store
	kv.last = last
	kv.cfg = cfg
}

func (kv *ShardKV) executor() {
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

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should sd snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	defCfgShards := [shardctrler.NShards]int{}
	for i := range defCfgShards {
		defCfgShards[i] = -1
	}
	kv := &ShardKV{
		me:           me,
		maxraftstate: maxraftstate,
		make_end:     make_end,
		gid:          gid,
		ctrlers:      ctrlers,
		applyCh:      applyCh,
		persister:    persister,
		rf:           raft.Make(servers, me, persister, applyCh),
		shards:       make([]Shard, shardctrler.NShards),
		last:         make(map[int64]Last),
		notify:       make(map[int]chan *Result),
		cfg:          shardctrler.Config{Num: 0, Shards: defCfgShards},
		sc:           shardctrler.MakeClerk(ctrlers),
	}
	kv.rf.SetAsyncInstallSnapshot(true)
	kv.rf.SetTag(fmt.Sprintf("%d", kv.gid))

	// Your initialization code here.
	for shard := 0; shard < shardctrler.NShards; shard++ {
		kv.shards[shard] = Shard{Store: make(map[string]string), State: Default}
	}

	kv.restoreSnapshot(persister.ReadSnapshot())
	log.Printf("ShardKV-%d%d [init] %v\n", kv.gid, kv.me, kv.cfg)

	go kv.configListener()
	go kv.shardTransmitter()
	go kv.executor()

	return kv
}
