package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskTypeMap    = 0
	TaskTypeReduce = 1
)

const (
	TaskUnassigned = -1
	TaskFinished   = -2
)

type TaskLog struct {
	Id     uint32
	Inputs []string

	Lock        sync.Mutex
	Worker      int
	WorkerStart time.Time
	Backup      int
	BackupStart time.Time
}

func (t *TaskLog) Done() bool {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	return t.Worker == TaskFinished
}

type Coordinator struct {
	// Your definitions here.
	MapTasks    []TaskLog
	ReduceTasks []TaskLog
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, _ *SubmitTaskReply) error {
	var tasks []TaskLog
	if args.Type == TaskTypeReduce {
		tasks = c.ReduceTasks
	} else {
		tasks = c.MapTasks
	}

	t := &tasks[args.Id]
	t.Lock.Lock()
	if t.Worker == TaskFinished {
		// already finished
		t.Lock.Unlock()
		return nil
	}
	t.Worker = TaskFinished
	t.Lock.Unlock()

	for i, result := range args.Results {
		c.ReduceTasks[i].Lock.Lock()
		c.ReduceTasks[i].Inputs = append(c.ReduceTasks[i].Inputs, result)
		c.ReduceTasks[i].Lock.Unlock()
	}
	return nil
}

func (c *Coordinator) scheduleMapTask(who uint16) *TaskLog {
	for {
		mapDone := true
		for i := range c.MapTasks {
			t := &c.MapTasks[i]
			t.Lock.Lock()
			if t.Worker == TaskUnassigned || (t.Worker > 0 && t.WorkerStart.Add(10*time.Second).Before(time.Now())) {
				t.Worker = int(who)
				t.WorkerStart = time.Now()
				t.Lock.Unlock()
				return t
			}
			mapDone = mapDone && t.Worker == TaskFinished
			t.Lock.Unlock()
		}
		if mapDone {
			break
		}
		time.Sleep(1 * time.Second)
	}

	return nil
}

func (c *Coordinator) scheduleReduceTask(who uint16) *TaskLog {
	for {
		reduceDone := true
		for i := range c.ReduceTasks {
			t := &c.ReduceTasks[i]
			t.Lock.Lock()
			if t.Worker == TaskUnassigned || (t.Worker > 0 && t.WorkerStart.Add(10*time.Second).Before(time.Now())) {
				t.Worker = int(who)
				t.WorkerStart = time.Now()
				t.Lock.Unlock()
				return t
			}
			reduceDone = reduceDone && t.Worker == TaskFinished
			t.Lock.Unlock()
		}
		if reduceDone {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := c.scheduleMapTask(args.Whoami)
	if task != nil {
		reply.Ok = true
		reply.Task = Task{
			Id:   task.Id,
			Type: TaskTypeMap,
			Detail: MapTask{
				NReduce: len(c.ReduceTasks),
				Input:   task.Inputs[0],
			},
		}
		return nil
	}

	{
		task := c.scheduleReduceTask(args.Whoami)
		if task != nil {
			reply.Ok = true
			reply.Task = Task{
				Id:   task.Id,
				Type: TaskTypeReduce,
				Detail: ReduceTask{
					Inputs: task.Inputs,
				},
			}
			return nil
		}
	}

	reply.Ok = false
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for i := 0; i < len(c.MapTasks); i++ {
		if !c.MapTasks[i].Done() {
			return false
		}
	}

	for i := 0; i < len(c.ReduceTasks); i++ {
		if !c.ReduceTasks[i].Done() {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:    make([]TaskLog, len(files)),
		ReduceTasks: make([]TaskLog, nReduce),
	}
	seq := uint32(42)
	for i := 0; i < len(files); i++ {
		c.MapTasks[i] = TaskLog{
			Id:          uint32(i),
			Inputs:      []string{files[i]},
			Worker:      TaskUnassigned,
			WorkerStart: time.Time{},
			Backup:      TaskUnassigned,
			BackupStart: time.Time{},
			Lock:        sync.Mutex{},
		}
		seq += 1
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = TaskLog{
			Id:          uint32(i),
			Inputs:      []string{},
			Worker:      TaskUnassigned,
			WorkerStart: time.Time{},
			Backup:      TaskUnassigned,
			BackupStart: time.Time{},
			Lock:        sync.Mutex{},
		}
		seq += 1
	}
	c.server()
	return &c
}
