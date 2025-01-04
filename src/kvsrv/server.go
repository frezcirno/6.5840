package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Last struct {
	Seq   int32
	Value string
}

type KVServer struct {
	mu    sync.Mutex
	store map[string]string
	last  map[int32]Last
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// log.Println("Get", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// log.Println("Put", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if last, ok := kv.last[args.ClientId]; ok {
		if args.Seq < last.Seq {
			panic("seq < last")
		}
		if args.Seq == last.Seq {
			return
		}
	}
	kv.last[args.ClientId] = Last{Seq: args.Seq}
	kv.store[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// log.Println("Append", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if last, ok := kv.last[args.ClientId]; ok {
		if args.Seq < last.Seq {
			panic("seq < last")
		}
		if args.Seq == last.Seq {
			reply.Value = last.Value
			return
		}
	}
	// Reply with the old value
	reply.Value = kv.store[args.Key]
	kv.last[args.ClientId] = Last{Seq: args.Seq, Value: kv.store[args.Key]}
	kv.store[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.last = make(map[int32]Last)
	return kv
}
