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

type KVServer struct {
	mu    sync.Mutex
	store map[string]string
	ack   map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if last, ok := kv.ack[args.Txn]; ok {
		reply.Value = last
		return
	}
	kv.ack[args.Txn] = kv.store[args.Key]
	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.ack[args.Txn]; ok {
		return
	}
	kv.store[args.Key] = args.Value
	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if last, ok := kv.ack[args.Txn]; ok {
		reply.Value = last
		return
	}
	kv.ack[args.Txn] = kv.store[args.Key]
	reply.Value = kv.store[args.Key]
	kv.store[args.Key] += args.Value
}

func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.ack, args.Txn)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.ack = make(map[int64]string)
	return kv
}
