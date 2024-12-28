package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"syscall"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GenWorkerId() uint16 {
	return (uint16)(os.Getpid() + syscall.Gettid())
}

func doMapTask(mapf func(string, string) []KeyValue, id uint32, mapTask MapTask) ([]string, error) {
	contents, err := os.ReadFile(mapTask.Input)
	if err != nil {
		log.Fatal("read file error:", err)
		return nil, err
	}
	mapResult := mapf(mapTask.Input, string(contents))

	intermediates := make([]string, mapTask.NReduce)
	for _, kv := range mapResult {
		intermediates[ihash(kv.Key)%int(mapTask.NReduce)] += kv.Key + " " + kv.Value + "\n"
	}

	files := make([]string, mapTask.NReduce)
	for i := 0; i < int(mapTask.NReduce); i++ {
		f, err := os.CreateTemp("", "mr-tmp-")
		if err != nil {
			log.Fatal("create temp file error:", err)
			return nil, err
		}
		f.WriteString(intermediates[i])
		f.Close()

		oname := fmt.Sprintf("mr-%d-%d", id, i)
		err = os.Rename(f.Name(), oname)
		if err != nil {
			log.Fatal("rename file error:", err)
			return nil, err
		}
		files[i] = oname
	}

	return files, nil
}

func doReduceTask(reducef func(string, []string) string, id uint32, reduceTask ReduceTask) error {
	// read the files.
	kvs := make([]KeyValue, 0)
	for _, input := range reduceTask.Inputs {
		f, err := os.Open(input)
		if err != nil {
			log.Fatal("open file error:", err)
			return err
		}
		defer f.Close()

		kv := KeyValue{}
		for {
			_, err := fmt.Fscanf(f, "%v %v", &kv.Key, &kv.Value)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("scan file error:", err)
				return err
			}
			kvs = append(kvs, kv)
		}
	}

	// sort the kvs.
	sort.Sort(ByKey(kvs))

	// output the kvs to a file.
	f, err := ioutil.TempFile("", "mr-tmp-")
	if err != nil {
		log.Fatal("create temp file error:", err)
		return err
	}

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	f.Close()

	// rename the file.
	oname := "mr-out-" + strconv.Itoa(int(id))
	os.Remove(oname)
	err = os.Rename(f.Name(), oname)
	if err != nil {
		log.Fatal("rename file error:", err)
		return err
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	id := GenWorkerId()

	// Your worker implementation here.
	for {
		task, err := fetchTask(id)
		if err != MapReduceOk {
			break
		}
		// log.Println("get task:", task)
		switch task.Type {
		case TaskTypeMap:
			outfiles, err := doMapTask(mapf, task.Id, task.Detail.(MapTask))
			if err != nil {
				log.Fatal("doMapTask failed:", err)
				break
			}
			err = submitTask(task, outfiles)
			if err != MapReduceOk {
				log.Fatal("submitTask failed:", err)
			}
		case TaskTypeReduce:
			err := doReduceTask(reducef, task.Id, task.Detail.(ReduceTask))
			if err != nil {
				log.Fatal("doReduceTask failed:", err)
				break
			}
			err = submitTask(task, nil)
			if err != MapReduceOk {
				log.Fatal("submitTask failed:", err)
			}
		}
	}
}

func submitTask(task *Task, results []string) MapReduceError {
	// declare an argument structure.
	args := SubmitTaskArgs{Id: task.Id, Type: task.Type, Results: results}

	// declare a reply structure.
	reply := SubmitTaskReply{}

	return callWithRetry("Coordinator.SubmitTask", &args, &reply)
}

func fetchTask(whoami uint16) (*Task, MapReduceError) {
	// declare an argument structure.
	args := GetTaskArgs{Whoami: whoami}

	// declare a reply structure.
	reply := GetTaskReply{}

	err := callWithRetry("Coordinator.GetTask", &args, &reply)
	if err != MapReduceOk {
		return nil, err
	}

	if !reply.Ok {
		return nil, ErrNoMoreTask
	}

	return &reply.Task, MapReduceOk
}

func callWithRetry(rpcname string, args interface{}, reply interface{}) MapReduceError {
	for i := 0; i < 3; i++ {
		if call(rpcname, args, reply) {
			return MapReduceOk
		}
	}
	return ErrRpcFailed
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
