package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int32
	Seq      int32
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int32
	Seq      int32
}

type GetReply struct {
	Value string
}

type AckArgs struct {
	ClientId int32
	Seq      int32
}

type AckReply struct {
}
