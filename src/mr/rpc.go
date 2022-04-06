package mr

import (
	"os"
	"strconv"
)

type TaskStatus int

const (
	NOTSTARTED TaskStatus = 0
	ASSIGNED   TaskStatus = 1
	SUCCESS    TaskStatus = 2
	FAIL       TaskStatus = 3
)

type TaskType int

const (
	MAPTASK    TaskType = 0
	REDUCETASK TaskType = 1
)

// TODO: this is empty, is
type GetTaskRequest struct {
}

type ChangeTaskStatusRequest struct {
	ID     Task
	status TaskStatus
}

type Task struct {
	ID        string
	ReduceNum int
}

type GetTaskResponse struct {
	TaskNotReady bool
	Type         TaskType
	TaskArg      interface{}
}

type MapTask struct {
	Task     *Task
	FileName string
}

type ReduceTask struct {
	Task  *Task
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
