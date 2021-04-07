package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const idle int = 0

const maptask int = 1
const reducetask int = 2

var workernum int = 0

const undergoing int = 1
const finished int = 2

type RegisterArgs struct {
	WorkerName int
}

type RegisterReply struct {
	WorkerName int
}

type AssignArgs struct {
}

type AssignReply struct {
	FileName string
	TaskType int
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
