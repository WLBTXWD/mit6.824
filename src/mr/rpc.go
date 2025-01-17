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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetReduceCountArgs struct{}

type GetReduceCountReply struct {
	ReduceCount int
}

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskId   int
	TaskFile string
}

type ReportTaskArgs struct {
	TaskType TaskType
	WorkId   int
	TaskId   int
}

type ReportTaskReply struct {
	CanExit bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
