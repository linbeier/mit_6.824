package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	Fileset        []string
	FilesetPointer int

	MapFinished bool
	MapTasks    map[string]int

	ReduceFinished bool
	ReduceTaks     map[string]int

	WorkerStatus map[string]int
	WorkerNum    int
}

// Your code here -- RPC handlers for the worker to call.

//
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	//give worker a name or pass though to reply
	if args.WorkerName == 0 {
		reply.WorkerName = c.WorkerNum
	} else {
		reply.WorkerName = args.WorkerName
	}

	return nil
}

func (c *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {
	if !c.MapFinished {
		reply.TaskType = maptask
		if c.FilesetPointer < len(c.Fileset) {
			reply.FileName = c.Fileset[c.FilesetPointer]
			c.MapTasks[reply.FileName] = undergoing

			return nil

		} else {
			return errors.New("no more file to be assigned")
		}

	} else if !c.ReduceFinished {
		reply.TaskType = reducetask

	}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Fileset = files
	c.FilesetPointer = 0
	c.MapTasks = make(map[string]int)
	c.MapFinished = false
	c.ReduceTaks = make(map[string]int)
	c.ReduceFinished = false
	c.WorkerStatus = make(map[string]int)
	c.WorkerNum = 1

	for _, v := range c.Fileset {
		c.MapTasks[v] = idle
	}

	c.server()
	return &c
}
