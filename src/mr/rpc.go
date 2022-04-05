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

type TaskStatus int

const (
	SUCCESS TaskStatus = 1
	FAIL    TaskStatus = 2
)

type TaskType int

const (
	MAPTASK    TaskType = 1
	REDUCETASK TaskType = 2
)

// TODO: this is empty, is
type GetTaskRequest struct {
}

type ChangeTaskStatusRequest struct {
	ID     TaskID
	status TaskStatus
}

type TaskID struct {
	ID string
}

type GetTaskResponse struct {
	TaskNotReady bool
	Type         TaskType
	TaskArg      interface{}
}

type MapTask struct {
	ID       TaskID
	FileName string
}

type ReduceTask struct {
	ID    TaskID
	Files []string
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
