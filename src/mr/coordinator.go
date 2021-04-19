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

type MapTask struct {
	m     map[int]TaskInfo
	mutex sync.RWMutex
}

type ReduceTask struct {
	r         map[int]TaskInfo
	mutex     sync.Mutex
	ReduceNum int
}

type Coordinator struct {
	// Your definitions here.
	nReduce int

	Fileset        []string
	FilesetPointer int

	MapTasks MapTask

	ReduceTasks ReduceTask

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
			TimeBegin: time.Now(),
		}

		reply.t.TaskNum = c.FilesetPointer
		c.MapTasks.mutex.Lock()
		c.MapTasks.m[reply.t.TaskNum] = reply.t
		c.MapTasks.mutex.Unlock()
		c.FilesetPointer++

	} else if len(c.MapTasks.m) > 0 {
		//map任务已分配完全，等待map任务全部完成
		iternum := -1
		timenow := time.Now()

		c.MapTasks.mutex.RLock()
		for i, v := range c.MapTasks.m {
			if timenow.Sub(v.TimeBegin) >= 60*time.Second {
				reply.t = TaskInfo{
					TaskType:  maptask,
					NReduce:   c.nReduce,
					TaskNum:   i,
					FileName:  v.FileName,
					TimeBegin: time.Now(),
				}
				iternum = i
				break
			}
		}
		c.MapTasks.mutex.RUnlock()

		if iternum == -1 {
			reply.t = TaskInfo{
				TaskType: idle,
			}
		} else {
			c.MapTasks.mutex.Lock()
			c.MapTasks.m[iternum] = reply.t
			c.MapTasks.mutex.Unlock()
		}

	} else {
		//reduce task
		//todo: check if reduce task ran out of date
		if c.ReduceTasks.ReduceNum < c.nReduce {
			reply.t = TaskInfo{
				TaskType:  reducetask,
				NReduce:   c.nReduce,
				TimeBegin: time.Now(),
			}
			reply.t.TaskNum = c.ReduceTasks.ReduceNum
			c.ReduceTasks.mutex.Lock()
			c.ReduceTasks.r[c.ReduceTasks.ReduceNum] = reply.t
			c.ReduceTasks.ReduceNum++
			c.ReduceTasks.mutex.Unlock()
		} else {
			reply.t = TaskInfo{
				TaskType: idle,
			}
		}
	}

	return nil
}

func (c *Coordinator) WorkFinish(args *FinishArgs, reply *FinishReply) error {
	//map task finish
	if args.TaskType == maptask {
		c.MapTasks.mutex.Lock()
		delete(c.MapTasks.m, args.TaskNum)
		c.MapTasks.mutex.Unlock()

	} else {
		c.ReduceTasks.mutex.Lock()
		delete(c.ReduceTasks.r, args.TaskNum)
		c.ReduceTasks.mutex.Unlock()

	}
	// carg := AssignArgs{}
	// creply := AssignReply{t: TaskInfo{}}
	// c.Assign(&carg, &creply)
	// reply.t = creply.t
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
	c.nReduce = nReduce

	c.MapTasks.m = make(map[int]TaskInfo)

	c.ReduceTasks.r = make(map[int]TaskInfo)
	c.ReduceTasks.ReduceNum = 0

	c.server()
	return &c
}
