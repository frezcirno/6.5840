package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/gob"
	"os"
	"strconv"
)

type MapReduceError int

func (e MapReduceError) Error() string {
	return strconv.Itoa(int(e))
}

const (
	MapReduceOk                       = MapReduceError(0)
	ErrNoMoreTask      MapReduceError = iota
	ErrUnmatchedWorker MapReduceError = iota
	ErrRpcFailed       MapReduceError = iota
	ErrTaskType        MapReduceError = iota
)

type MapTask struct {
	NReduce int
	Input   string
}

type ReduceTask struct {
	Inputs []string
}

type Task struct {
	Id     uint32
	Type   uint8
	Detail interface{}
	SeqNo  uint16
}

// example to show how to declare the arguments
// and reply for an RPC.
type GetTaskArgs struct {
	Whoami uint16
}

type GetTaskReply struct {
	Ok   bool
	Task Task
}

type SubmitTaskArgs struct {
	Id      uint32
	Type    uint8
	Results []string
}

type SubmitTaskReply struct {
}

func init() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
}

// Add your RPC definitions here.
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
