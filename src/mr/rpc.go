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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterArgs struct {
	WorkerName string
	MaxFilenum int
}

type RegisterReply struct {
	FileNames  []string
	TaskType   int
	WorkerName string
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
