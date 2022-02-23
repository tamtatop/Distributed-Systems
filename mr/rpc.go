package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const MapTask = 0
const ReduceTask = 1
const WaitForTask = 2
const NoMoreTasks = 3

type TaskAssign struct {
	TaskType int
	FileName string // only relevant if TaskType = MapTask
	TaskIndex int
	NReduce int // only relevant if TaskType = MapTask
	NMap int // only relevant if TaskType = ReduceTask
}

type TaskFinished struct {
	TaskType int
	TaskIndex int
}

// not my code
type ExampleArgs struct {
	X int
}


type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
