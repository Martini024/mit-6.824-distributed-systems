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

type Coordinator struct {
	// Your definitions here.
	mu                  sync.Mutex
	MapTasks            []MapTask
	MapTasksFinished    bool
	ReduceTasks         []ReduceTask
	ReduceTasksFinished bool
	Region              int
}

type MapTask struct {
	File     string
	Assigned bool
	Finished bool
}

type ReduceTask struct {
	Files    []string
	Assigned bool
	Finished bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestForTaskHandler(args *struct{}, reply *RequestForTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 1. Check if there's map task unfinished
	if !c.MapTasksFinished {
		// 1.1 Fetch an unassigned map task
		for i, mapTask := range c.MapTasks {
			if !mapTask.Assigned {
				// 1.2 Reply to worker with map task info
				reply.Id = i
				reply.Type = TaskType(Map)
				reply.Files = []string{mapTask.File}
				reply.Region = c.Region
				c.MapTasks[i].Assigned = true
				return nil
			}
		}
	} else if !c.ReduceTasksFinished {
		// 2.1 Fetch an unassigned reduce task
		for i, reduceTask := range c.ReduceTasks {
			if !reduceTask.Assigned {
				// 2.2 Reply to worker with reduce task info
				reply.Id = i
				reply.Type = TaskType(Reduce)
				reply.Files = reduceTask.Files
				c.ReduceTasks[i].Assigned = true
				return nil
			}
		}
	}
	return nil
}

func (c *Coordinator) FinishTaskHandler(args *FinishTaskArgs, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case Map:
		c.MapTasks[args.Id].Finished = true
		for _, mapTask := range c.MapTasks {
			if !mapTask.Finished {
				return nil
			}
		}
		c.MapTasksFinished = true
		for i := 0; i < c.Region; i++ {
			files := []string{}
			for j := 0; j < len(c.MapTasks); j++ {
				filename := fmt.Sprint("mr-", j, "-", i)
				files = append(files, filename)
			}
			c.ReduceTasks = append(c.ReduceTasks, ReduceTask{files, false, false})
		}
	case Reduce:
		c.ReduceTasks[args.Id].Finished = true
		for _, reduceTask := range c.ReduceTasks {
			if !reduceTask.Finished {
				return nil
			}
		}
		c.ReduceTasksFinished = true
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.MapTasksFinished && c.ReduceTasksFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapTasks = []MapTask{}
	c.Region = nReduce
	for _, file := range files {
		c.MapTasks = append(c.MapTasks, MapTask{file, false, false})
	}

	c.server()
	return &c
}
