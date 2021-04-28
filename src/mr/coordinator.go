package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type MapTask struct {
	m     map[int]TaskInfo
	mutex sync.RWMutex
}

type ReduceTask struct {
	r         map[int]TaskInfo
	mutex     sync.RWMutex
	ReduceNum int
}

type Coordinator struct {
	// Your definitions here.
	nReduce int

	Fileset        []string
	FilesetPointer int

	MapTasks MapTask

	ReduceTasks ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

//
func (c *Coordinator) Assign(args *AssignArgs, reply *AssignReply) error {
	//map任务还未分配完全
	//data race
	c.MapTasks.mutex.RLock()
	if c.FilesetPointer < len(c.Fileset) {

		c.MapTasks.mutex.RUnlock()

		reply.T = TaskInfo{
			TaskType:  maptask,
			NReduce:   c.nReduce,
			FileName:  c.Fileset[c.FilesetPointer],
			TimeBegin: time.Now(),
		}

		reply.T.TaskNum = c.FilesetPointer
		c.MapTasks.mutex.Lock()
		c.MapTasks.m[reply.T.TaskNum] = reply.T
		c.FilesetPointer++
		c.MapTasks.mutex.Unlock()
		//data race
	} else if len(c.MapTasks.m) > 0 {

		c.MapTasks.mutex.RUnlock()

		//map任务已分配完全，等待map任务全部完成
		iternum := -1
		timenow := time.Now()

		c.MapTasks.mutex.RLock()
		for i, v := range c.MapTasks.m {
			if timenow.Sub(v.TimeBegin) >= 10*time.Second {
				reply.T = TaskInfo{
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
			reply.T = TaskInfo{
				TaskType: idle,
			}
		} else {
			c.MapTasks.mutex.Lock()
			c.MapTasks.m[iternum] = reply.T
			c.MapTasks.mutex.Unlock()
		}

	} else {
		//reduce task
		//data race

		c.MapTasks.mutex.RUnlock()
		c.ReduceTasks.mutex.RLock()

		if c.ReduceTasks.ReduceNum < c.nReduce {

			c.ReduceTasks.mutex.RUnlock()

			reply.T = TaskInfo{
				TaskType:  reducetask,
				NReduce:   c.nReduce,
				TimeBegin: time.Now(),
			}
			reply.T.TaskNum = c.ReduceTasks.ReduceNum
			c.ReduceTasks.mutex.Lock()
			c.ReduceTasks.r[c.ReduceTasks.ReduceNum] = reply.T
			c.ReduceTasks.ReduceNum++
			c.ReduceTasks.mutex.Unlock()
			//data race
		} else if len(c.ReduceTasks.r) > 0 {

			c.ReduceTasks.mutex.RUnlock()

			iternum := -1
			timenow := time.Now()

			c.ReduceTasks.mutex.RLock()
			for i, v := range c.ReduceTasks.r {
				if timenow.Sub(v.TimeBegin) >= 10*time.Second {
					reply.T = TaskInfo{
						TaskType:  reducetask,
						NReduce:   c.nReduce,
						TaskNum:   i,
						TimeBegin: time.Now(),
					}
					iternum = i
					break
				}
			}
			c.ReduceTasks.mutex.RUnlock()

			if iternum > -1 {
				c.ReduceTasks.mutex.Lock()
				c.ReduceTasks.r[iternum] = reply.T
				c.ReduceTasks.mutex.Unlock()
			}
		} else {

			c.ReduceTasks.mutex.RUnlock()

			reply.T = TaskInfo{
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
	} else if args.TaskType == reducetask {
		c.ReduceTasks.mutex.Lock()
		delete(c.ReduceTasks.r, args.TaskNum)
		c.ReduceTasks.mutex.Unlock()

		var filenames []string

		regstring := "^mr-.-" + strconv.Itoa(args.TaskNum) + "-"
		Dirfiles, err := ioutil.ReadDir(tempdir)
		if err != nil {
			fmt.Println(err)
		}
		for _, file := range Dirfiles {
			filenames = append(filenames, file.Name())
		}
		MatchedName := RegPrefixMatch(regstring, filenames)

		for _, name := range MatchedName {
			err = os.Remove(name)
			if err != nil {
				fmt.Println(err)
			}
		}
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

	c.ReduceTasks.mutex.RLock()
	c.MapTasks.mutex.RLock()
	if c.FilesetPointer >= len(c.Fileset) && len(c.MapTasks.m) == 0 && len(c.ReduceTasks.r) == 0 && c.ReduceTasks.ReduceNum == c.nReduce {
		ret = true
	}
	c.MapTasks.mutex.RUnlock()
	c.ReduceTasks.mutex.RUnlock()

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

	c.MapTasks = MapTask{}
	c.MapTasks.m = make(map[int]TaskInfo)

	c.ReduceTasks = ReduceTask{}
	c.ReduceTasks.r = make(map[int]TaskInfo)
	c.ReduceTasks.ReduceNum = 0

	c.server()
	return &c
}
