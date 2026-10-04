package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "fmt"

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

type TaskType int

const (
	WaitTask TaskType = iota // zero value, so an unfilled reply means "wait"
	MapTask
	ReduceTask
	ExitTask // job finished; the worker should exit
)

func (t TaskType) String() string {
	switch t {
	case WaitTask:
		return "wait"
	case MapTask:
		return "map"
	case ReduceTask:
		return "reduce"
	case ExitTask:
		return "exit"
	}
	return fmt.Sprintf("TaskType(%d)", int(t))
}

type TaskArgs struct {
}

type TaskReply struct {
	TaskType TaskType
	FileName string // map tasks only
	TaskID   int    // map task index or reduce bucket
	NMap     int
	NReduce  int
}

type ReportTaskArgs struct {
	TaskID   int
	TaskType TaskType
}

type ReportTaskReply struct{}
