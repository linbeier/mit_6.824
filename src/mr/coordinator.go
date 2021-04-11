package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce int

	Fileset        []string
	FilesetPointer int

	MapTasks []TaskInfo

	ReduceTaks []TaskInfo

	WorkerStatus []TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
func (c *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {
	//map任务还未分配完全
	if c.FilesetPointer < len(c.Fileset) {
		reply.t = TaskInfo{
			TaskType:  maptask,
			NReduce:   c.nReduce,
			FileName:  c.Fileset[c.FilesetPointer],
			TimeBegin: args.TimeBegin,
		}
		c.FilesetPointer++
		c.MapTasks = append(c.MapTasks, reply.t)
		reply.t.TaskNum = len(c.MapTasks) - 1
	} else if len(c.MapTasks) > 0 {
		//map任务已分配完全，等待map任务全部完成
		//todo: Check if task in queue out-date
		timenow := time.Now()
		for i, v := range c.MapTasks {
			if timenow.Sub(v.TimeBegin) >= 60*time.Second {
				reply.t = TaskInfo{
					TaskType:  maptask,
					NReduce:   c.nReduce,
					TaskNum:   i,
					FileName:  v.FileName,
					TimeBegin: time.Now(),
				}
				c.MapTasks[i] = reply.t
				break
			} else {
				reply.t = TaskInfo{
					TaskType: idle,
				}
			}
		}

	} else {
		//reduce task
		if len(c.ReduceTaks) < c.nReduce {
			reply.t = TaskInfo{
				TaskType:  reducetask,
				NReduce:   c.nReduce,
				TimeBegin: args.TimeBegin,
			}
			c.ReduceTaks = append(c.ReduceTaks, reply.t)
			reply.t.TaskNum = len(c.ReduceTaks) - 1
		} else {
			reply.t = TaskInfo{
				TaskType: idle,
			}
		}
	}

	return nil
}

func (c *Coordinator) WorkFinish() error {

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
	c.nReduce = nReduce

	c.server()
	return &c
}
