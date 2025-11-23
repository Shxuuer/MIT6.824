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

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

type RespTask struct {
	TaskType TaskType
	TaskID   int
	Filename string
	NReduce  int
	NMap     int
}

type ReqFinished struct {
	TaskType TaskType
	TaskID   int
}

type ReqRespNone struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
