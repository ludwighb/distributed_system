package mr

import (
	"os"
	"strconv"
	"time"
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

type AllTaskStatus int

const (
	TASKNOTREAQDY AllTaskStatus = 0
	TASKASSIGNED  AllTaskStatus = 1
	TASKSALLDONE  AllTaskStatus = 2
)

// TODO: this is empty, is
type GetTaskRequest struct {
}

type ChangeTaskStatusRequest struct {
	Name   string
	Status TaskStatus
}

// TODO: for this request i think it doesn't need any respose info, right?
type ChangeTaskStatusResponse struct {
}

type Task struct {
	// string name for task. eg: map-1
	Name string
	// the ith map/or reduce task
	ID        int
	ReduceNum int
	Status    TaskStatus
	// TODO: put these files here for now, but need to move them to coordinator because worker doesn't need to know this
	TaskAssignedTimestamp time.Time
	AssignedWorkerId      int
}

type GetTaskResponse struct {
	STATUS  AllTaskStatus
	Type    TaskType
	TaskArg interface{}
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
