package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	me     int64
	seq    int64
	leader map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.me = nrand()
	ck.seq = 0
	ck.config.Num = -1
	return ck
}

func (ck *Clerk) NextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

// ask controller for the latest configuration.
func (ck *Clerk) updateConfig() {
	cfg := ck.sm.Query(-1)
	if cfg.Num != ck.config.Num {
		ck.config = cfg
		ck.leader = make(map[int]int)
		for gid := range ck.config.Groups {
			ck.leader[gid] = 0
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.ClientId = ck.me
	args.Seq = ck.NextSeq()
	args.Key = key
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers := ck.config.Groups[gid]; len(servers) > 0 {
			for {
				var reply GetReply
				ok := ck.make_end(servers[ck.leader[gid]]).Call("ShardKV.Get", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout || reply.Err == ErrBusy {
					ck.leader[gid] = (ck.leader[gid] + 1) % len(servers)
				} else if reply.Err == OK {
					return reply.Value
				} else if reply.Err == ErrWrongGroup {
					// try next group
					break
				}

				time.Sleep(20 * time.Millisecond)
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.updateConfig()
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.ClientId = ck.me
	args.Seq = ck.NextSeq()
	args.Key = key
	args.Value = value
	args.Op = op
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers := ck.config.Groups[gid]; len(servers) > 0 {
			for {
				var reply PutAppendReply
				ok := ck.make_end(servers[ck.leader[gid]]).Call("ShardKV.PutAppend", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrBusy || reply.Err == ErrTimeout {
					ck.leader[gid] = (ck.leader[gid] + 1) % len(servers)
				} else if reply.Err == OK {
					return
				} else if reply.Err == ErrWrongGroup {
					// try next group
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.updateConfig()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
