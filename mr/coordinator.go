package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type mapTask struct {
	done     bool
	worker   int
	filename string
}

type reduceTask struct {
	done   bool
	worker int
}

type Coordinator struct {
	mapTasks    []mapTask
	reduceTasks []reduceTask

	availableMapTasks    map[int]int
	availableReduceTasks map[int]int

	mapDone    bool
	reduceDone bool

	mapLock    sync.Mutex
	reduceLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *GetArgs, reply *GetReply) error {
	fmt.Printf("Worker %v requesting for task\n", args.WorkerId)
	// If map task available, send map task

	c.mapLock.Lock()
	for id, _ := range c.availableMapTasks {
		defer fmt.Printf("Map Task %v given to worker %v\n", id, args.WorkerId)

		// Populate reply
		reply.TaskType = 0
		reply.TaskNum = id
		reply.Filename = c.mapTasks[id].filename

		// Fill in maptask details
		c.mapTasks[id].worker = args.WorkerId

		// Remove from available
		delete(c.availableMapTasks, id)

		// Run waiting thread
		// defer go waitFor10Sec()

		c.mapLock.Unlock()

		return nil
	}
	c.mapLock.Unlock()

	// Not task available right now
	if !c.mapDone {
		fmt.Printf("No tasks available for worker %v\n", args.WorkerId)
		reply.TaskType = 2
		return nil
	}

	// TODO
	// If all map tasks over and reduce task available send reduce task
	reply.TaskType = 2
	return nil

	// Not task available right now
}

func (c *Coordinator) FinishTask(args *FinishArgs, reply *FinishReply) error {
	// TODO: Add locks

	if args.TaskType == 0 {
		// Map task finished
		c.mapLock.Lock()
		defer c.mapLock.Unlock()
		if c.mapTasks[args.TaskNum].worker == args.WorkerId {
			fmt.Printf("Worker %v finished map task %v\n",
				args.WorkerId, args.TaskNum)
			c.mapTasks[args.TaskNum].done = true
			c.mapTasks[args.TaskNum].worker = -1
		} else {
			fmt.Printf("Worker %v finished map task %v too late\n",
				args.WorkerId, args.TaskNum)
		}
		return nil
	} else {
		// Reduce task finished
		c.reduceLock.Lock()
		defer c.reduceLock.Unlock()
		if c.reduceTasks[args.TaskNum].worker == args.WorkerId {
			fmt.Printf("Worker %v finished reduce task %v\n",
				args.WorkerId, args.TaskNum)
			c.reduceTasks[args.TaskNum].done = true
			c.reduceTasks[args.TaskNum].worker = -1
		} else {
			fmt.Printf("Worker %v finished reduce task %v too late\n",
				args.WorkerId, args.TaskNum)
		}
		return nil
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := c.mapDone && c.reduceDone
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mapDone, c.reduceDone = false, false

	// Fill map tasks
	c.mapTasks = make([]mapTask, len(files))
	c.availableMapTasks = make(map[int]int)

	for i, _ := range c.mapTasks {
		c.mapTasks[i] = mapTask{false, -1, files[i]}
		c.availableMapTasks[i] = i
	}

	// Fill reduce tasks
	c.reduceTasks = make([]reduceTask, nReduce)
	c.availableReduceTasks = make(map[int]int)

	for i, _ := range c.reduceTasks {
		c.reduceTasks[i] = reduceTask{false, -1}
		c.availableReduceTasks[i] = i
	}

	c.server()
	return &c
}
