package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const ExecuteTimeout = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead         int32 // set by Kill()
	last         map[int64]*Last
	notify       map[int]chan *Result
	lastExecuted int

	configs []Config // indexed by config num
}

type Result struct {
	Err   Err
	Value interface{}
}

type Last struct {
	Seq    int64
	Result *Result
}

type OpType uint8

const (
	OpQuery OpType = iota
	OpJoin
	OpLeave
	OpMove
)

type Op struct {
	// Your data here.
	ClientId int64
	Seq      int64
	Op       OpType
	// Query
	Num int
	// Join
	Replicas map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
}

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func (sc *ShardCtrler) isDup(clientId, seq int64) bool {
	last, ok := sc.last[clientId]
	if !ok {
		return false
	}
	assert(seq >= last.Seq, "seq < last.seq")
	return seq == last.Seq
}

func (sc *ShardCtrler) getNotify(index int) chan *Result {
	if _, ok := sc.notify[index]; !ok {
		sc.notify[index] = make(chan *Result, 1)
	}
	return sc.notify[index]
}

func (sc *ShardCtrler) purpose(op *Op) (res interface{}, err Err) {
	logIndex, _, leader := sc.rf.Start(*op)
	if !leader {
		return "", ErrWrongLeader
	}

	// log.Printf("ShardCtrler-%d [purpose] %v\n", sc.me, *op)

	sc.mu.Lock()
	notify := sc.getNotify(logIndex)
	sc.mu.Unlock()

	// wait for the result
	select {
	case r := <-notify:
		res, err = r.Value, r.Err
	case <-time.After(ExecuteTimeout):
		// timeout, ask client to poll again later
		err = ErrTimeout
	}

	// clean up
	go func() {
		sc.mu.Lock()
		delete(sc.notify, logIndex)
		sc.mu.Unlock()
	}()

	return
}

// Add new groups
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.RLock()
	if sc.isDup(args.ClientId, args.Seq) {
		reply.Err = sc.last[args.ClientId].Result.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	_, reply.Err = sc.purpose(&Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       OpJoin,
		Replicas: args.Servers,
	})
}

// Destroy groups
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.RLock()
	if sc.isDup(args.ClientId, args.Seq) {
		reply.Err = sc.last[args.ClientId].Result.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	_, reply.Err = sc.purpose(&Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       OpLeave,
		GIDs:     args.GIDs,
	})
}

// Move shards from one group to another
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.RLock()
	if sc.isDup(args.ClientId, args.Seq) {
		reply.Err = sc.last[args.ClientId].Result.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	_, reply.Err = sc.purpose(&Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       OpMove,
		Shard:    args.Shard,
		GID:      args.GID,
	})
}

// Query the configuration
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	res, err := sc.purpose(&Op{
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Op:       OpQuery,
		Num:      args.Num,
	})
	reply.Err = err
	if err == OK {
		reply.Config = res.(Config)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func group2Shards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}

func findGIDWithFewestShards(g2s map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(g2s[gid]) < min {
			index, min = gid, len(g2s[gid])
		}
	}
	return index
}

func findGIDWithMaxShards(g2s map[int][]int) int {
	// always choose gid 0 if there is any
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}
	// make iteration deterministic
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(g2s[gid]) > max {
			index, max = gid, len(g2s[gid])
		}
	}
	return index
}

func (sc *ShardCtrler) execute(op *Op) (result *Result) {
	result = &Result{Err: OK}

	if op.Op != OpQuery {
		if sc.isDup(op.ClientId, op.Seq) {
			return sc.last[op.ClientId].Result
		}
		defer func() {
			sc.last[op.ClientId] = &Last{Seq: op.Seq, Result: result}
		}()
	}

	switch op.Op {
	case OpQuery:
		if op.Num < 0 {
			op.Num = len(sc.configs) + op.Num
		}
		if op.Num > len(sc.configs)-1 {
			op.Num = len(sc.configs) - 1
		}
		result.Value = sc.configs[op.Num]

	case OpJoin:
		last := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    len(sc.configs),
			Shards: last.Shards,
			Groups: deepCopy(last.Groups),
		}

		// Add new Groups
		for gid, servers := range op.Replicas {
			_, ok := newConfig.Groups[gid]
			assert(!ok, "GID already exists")
			newConfig.Groups[gid] = servers
		}

		// Rebalance shards
		g2s := group2Shards(newConfig)
		for {
			maxShardsGID, minShardsGID := findGIDWithMaxShards(g2s), findGIDWithFewestShards(g2s)
			if maxShardsGID != 0 && len(g2s[maxShardsGID])-len(g2s[minShardsGID]) <= 1 {
				break
			}
			// move one shard from GID with most shards to GID with fewest shards
			g2s[minShardsGID] = append(g2s[minShardsGID], g2s[maxShardsGID][0])
			g2s[maxShardsGID] = g2s[maxShardsGID][1:]
		}

		for gid, shards := range g2s {
			for _, shard := range shards {
				newConfig.Shards[shard] = gid
			}
		}

		sc.configs = append(sc.configs, newConfig)

	case OpLeave:
		last := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    len(sc.configs),
			Shards: last.Shards,
			Groups: deepCopy(last.Groups),
		}

		// Destroy Groups
		g2s := group2Shards(newConfig)
		orphanedShards := make([]int, 0)
		for _, gid := range op.GIDs {
			_, ok := newConfig.Groups[gid]
			assert(ok, "GID does not exist")

			delete(newConfig.Groups, gid)

			if shards, ok := g2s[gid]; ok {
				orphanedShards = append(orphanedShards, shards...)
				delete(g2s, gid)
			}
		}

		// Rebalance shards
		if len(newConfig.Groups) != 0 {
			for _, shard := range orphanedShards {
				// Put orphaned shards to GID with fewest shards
				target := findGIDWithFewestShards(g2s)
				g2s[target] = append(g2s[target], shard)
			}

			for gid, shards := range g2s {
				for _, shard := range shards {
					newConfig.Shards[shard] = gid
				}
			}
		} else {
			// No groups left, assign all shards to group -1
			newConfig.Shards = [NShards]int{}
		}

		sc.configs = append(sc.configs, newConfig)

	case OpMove:
		last := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num: len(sc.configs),
			// [shard]gid
			Shards: last.Shards,
			// map[gid][]servers
			Groups: deepCopy(last.Groups),
		}

		newConfig.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, newConfig)

	}
	return result
}

func (sc *ShardCtrler) executor() {
	for !sc.killed() {
		msg := <-sc.applyCh
		assert(msg.CommandValid, "invalid command")

		op := msg.Command.(Op)
		sc.mu.Lock()

		// skip if the log applied is already executed
		if msg.CommandIndex <= sc.lastExecuted {
			sc.mu.Unlock()
			continue
		}
		assert(msg.CommandIndex == sc.lastExecuted+1 || sc.lastExecuted == 0, "unexpected command index")
		sc.lastExecuted = msg.CommandIndex

		result := sc.execute(&op)

		// only notify related channel for currentTerm's log when node is leader
		currentTerm, isLeader := sc.rf.GetState()
		if isLeader && msg.CommandTerm == currentTerm {
			ch := sc.getNotify(msg.CommandIndex)
			ch <- result
		}

		// log.Printf("ShardCtrler-%d [execute] %v -> %v\n", sc.me, op, *result)

		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.last = make(map[int64]*Last)
	sc.notify = make(map[int]chan *Result)
	sc.lastExecuted = 0

	go sc.executor()

	return sc
}
