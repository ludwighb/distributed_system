package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

// TODO: Cahnge to just use one lock
type Coordinator struct {
	// Your definitions here.
	mapTasks      []*MapTask
	reduceTasks   []*ReduceTask
	nameToTaskMap map[string]interface{}

	// TODO: do i need a lock for this?
	mu     sync.Mutex
	status AllTaskStatus
}

// why only data race in map task?
func (c *Coordinator) GetTaskHandler(req *GetTaskRequest, resp *GetTaskResponse) error {
	taskAssigned := false
	mapTaskAllDone := true
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, task := range c.mapTasks {
		// read
		if task.Task.Status != SUCCESS {
			mapTaskAllDone = false
		}

		if task.Task.Status == NOTSTARTED || task.Task.Status == FAIL {
			taskAssigned = true
			// write
			// TODO: make sure this assigns to the correct value.
			task.Task.Status = ASSIGNED
			resp.TaskArg = task
			resp.Type = MAPTASK
			// TODO: do we need to pass status to WORKER?
			resp.STATUS = TASKASSIGNED
			fmt.Printf("map task %v is assigned\n", task.Task.ID)
			return nil
		}
	}

	// TODO: merge this repeated code.
	reduceTaskAllDone := true
	if mapTaskAllDone {
		for _, task := range c.reduceTasks {
			if task.Task.Status != SUCCESS {
				reduceTaskAllDone = false
			}
			if task.Task.Status == NOTSTARTED || task.Task.Status == FAIL {
				taskAssigned = true
				task.Task.Status = ASSIGNED
				resp.TaskArg = task
				resp.Type = REDUCETASK
				resp.STATUS = TASKASSIGNED
				fmt.Printf("reduce task %v is assigned\n", task.Task.ID)
				return nil
			}
		}
	}

	if mapTaskAllDone && reduceTaskAllDone {
		resp.STATUS = TASKSALLDONE
		c.status = TASKSALLDONE
	} else if !taskAssigned {
		resp.STATUS = TASKNOTREAQDY
	}
	return nil
}

func (c *Coordinator) ChangeTaskStatusHandler(req *ChangeTaskStatusRequest, resp *ChangeTaskStatusResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	task, ok := c.nameToTaskMap[req.Name]
	if !ok {
		return fmt.Errorf("couldn't find task name %v", req.Name)
	}
	if strings.Contains(req.Name, "map") {
		task.(*MapTask).Task.Status = req.Status
	} else {
		task.(*ReduceTask).Task.Status = req.Status
	}
	fmt.Printf("Task %v status changed to %v\n", req.Name, req.Status)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	var taskDone AllTaskStatus
	c.mu.Lock()
	defer c.mu.Unlock()
	taskDone = c.status

	return taskDone == TASKSALLDONE
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := []*MapTask{}
	reduceTasks := []*ReduceTask{}
	nameToTaskMap := map[string]interface{}{}
	for i, file := range files {
		name := fmt.Sprintf("map-task-%v", i)
		mapTask := &MapTask{
			FileName: file,
			Task: &Task{
				Name:      name,
				ID:        i,
				ReduceNum: nReduce,
				Status:    NOTSTARTED,
			},
		}
		mapTasks = append(mapTasks, mapTask)
		nameToTaskMap[name] = mapTask
	}

	for i := 0; i < nReduce; i++ {
		fileList := []string{}
		for j := 0; j < len(files); j++ {
			intermediateFileName := fmt.Sprintf("mr-%v-%v.json", j, i)
			fileList = append(fileList, intermediateFileName)
		}
		name := fmt.Sprintf("reduce-task-%v", i)
		reduceTask := &ReduceTask{
			Files: fileList,
			Task: &Task{
				Name:      name,
				ID:        i,
				ReduceNum: nReduce,
				Status:    NOTSTARTED,
			},
		}
		reduceTasks = append(reduceTasks, reduceTask)
		nameToTaskMap[name] = reduceTask
	}

	c := Coordinator{
		mapTasks:      mapTasks,
		reduceTasks:   reduceTasks,
		nameToTaskMap: nameToTaskMap,
	}

	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

	// Your code here.
	c.server()
	return &c
}
