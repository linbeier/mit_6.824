package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const (
	idle       int = 0
	maptask    int = 1
	reducetask int = 2
)

type TaskInfo struct {
	TaskType  int
	TaskNum   int
	NReduce   int
	FileName  string
	TimeBegin time.Time
}

type AssignArgs struct {
}

type AssignReply struct {
	t TaskInfo
}

type FinishArgs struct {
	TaskType int
	TaskNum  int
}

type FinishReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
