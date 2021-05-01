package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Returns True if success else returns false
func mapFunc(mapf func(string, string) []KeyValue,
	taskNum int,
	filename string) bool {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return false
	}
	file.Close()
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))
	oname := "tmp/mr-" + strconv.Itoa(taskNum) + "-0"
	ofile, _ := os.Create(oname)

	for key, val := range kva {
		fmt.Fprintf(ofile, "%v %v\n", key, val)
	}

	return true
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	rand.Seed(time.Now().UnixNano())
	workerId := int(rand.Int31())

	for {
		// Ask master for task
		switch reply := callGetTask(workerId); reply.TaskType {
		case 0:
			fmt.Printf("[Worker %v]: Recieved map task %v from master\n",
				workerId, reply.TaskNum)
			ok := mapFunc(mapf, reply.TaskNum, reply.Filename)
			if ok {
				callFinishTask(reply.TaskType, workerId, reply.TaskNum)
			}
			// Map
		case 1:
			fmt.Printf("[Worker %v]: Recieved reduce task %v from master\n",
				reply.TaskNum, workerId)
			// Reduce
		default:
			// Sleep for 3 seconds
			fmt.Printf("[Worker %v]: No available task from master\n", workerId)
			time.Sleep(3 * time.Second)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func callGetTask(workerId int) *GetReply {
	args, reply := GetArgs{workerId}, GetReply{}

	ok := call("Coordinator.GetTask", &args, &reply)

	if !ok {
		fmt.Printf("Could not reach master\n")
		reply.TaskType = 99
		return &reply
	}

	return &reply
}

func callFinishTask(taskType, workerId, taskId int) {
	args, reply := FinishArgs{taskType, workerId, taskId}, FinishReply{}
	call("Coordinator.FinishTask", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
