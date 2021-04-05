package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const idle int = 0
const maptask int = 1
const reducetask int = 2

type Coordinator struct {
	// Your definitions here.
	Fileset        []string
	FilesetPointer int
	MapWorkers     []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	if c.FilesetPointer < len(c.Fileset) {
		if c.FilesetPointer+args.MaxFilenum < len(c.Fileset) {
			reply.FileNames = c.Fileset[c.FilesetPointer : c.FilesetPointer+args.MaxFilenum]
			c.FilesetPointer += args.MaxFilenum
		} else {
			reply.FileNames = c.Fileset[c.FilesetPointer:]
			c.FilesetPointer = len(c.Fileset)
		}
		c.MapWorkers = append(c.MapWorkers, args.WorkerName)
		return nil
	} else {
		return errors.New("no more file to be assigned")
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
	c.MapWorkers = []string{}

	c.server()
	return &c
}
