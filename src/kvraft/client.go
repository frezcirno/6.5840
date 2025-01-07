package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me     int64
	seq    int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.seq = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) NextSeq() int64 {
	ck.seq++
	return ck.seq
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	seq := ck.NextSeq()
	args := GetArgs{Key: key, ClientId: ck.me, Seq: seq}
	for {
		lastLeader := ck.leader
		for i := range ck.servers {
			server := ck.servers[(lastLeader+i)%len(ck.servers)]
			reply := GetReply{}
			if server.Call("KVServer.Get", &args, &reply) {
				if reply.Err == OK {
					ck.leader = (lastLeader + i) % len(ck.servers)
					return reply.Value
				}
			}
		}
		// log.Println("Get", seq, "failed, retrying")
		time.Sleep(100 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	seq := ck.NextSeq()
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.me, Seq: seq}
	for {
		lastLeader := ck.leader
		for i := range ck.servers {
			server := ck.servers[(lastLeader+i)%len(ck.servers)]
			reply := PutAppendReply{}
			if server.Call("KVServer."+op, &args, &reply) {
				if reply.Err == OK {
					ck.leader = (lastLeader + i) % len(ck.servers)
					return
				}
			}
		}
		// log.Println("PutAppend", seq, "failed, retrying")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
