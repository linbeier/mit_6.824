package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Taskinfo struct {
	Tasktype  int
	Tasknum   int
	NReduce   int
	FileName  string
	TimeBegin time.Time
}

type Coordinator struct {
	// Your definitions here.
	Fileset        []string
	FilesetPointer int

	MapTasks []Taskinfo

	ReduceTaks []Taskinfo

	WorkerStatus []Taskinfo
}

// Your code here -- RPC handlers for the worker to call.

//
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	//give worker a name or pass though to reply

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

	for i, v := range c.Fileset {
		c.MapTasks = append(c.MapTasks, Taskinfo{
			TaskType: maptask,
			Tasknum:  i,
			NReduce:  nReduce,
			FileName: v,
		})
	}

	c.server()
	return &c
}
