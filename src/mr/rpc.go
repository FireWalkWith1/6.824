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
type TaskArgs struct {
}

type TaskReply struct {
	Continue   bool
	Work       bool // 是否有任务
	NMap       int
	NReduce    int
	TaskType   string // 任务类型 map reduce
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type ResultArgs struct {
	TaskType string
	Index    int
	Result   bool
}

type ResultReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
