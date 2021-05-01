package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapFunc(mapf func(string, string) []KeyValue,
	taskType int,
	filename string) {
	time.Sleep(5 * time.Second)
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
			fmt.Printf("[Worker %v]: Recieved map task from master\n", workerId)
			mapFunc(mapf, reply.TaskNum, reply.Filename)
			callFinishTask(reply.TaskType, workerId, reply.TaskNum)
			// Map
		case 1:
			fmt.Printf("[Worker %v]: Recieved reduce task from master\n",
				workerId)
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
