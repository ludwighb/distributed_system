package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// TODO: Cahnge to just use one lock
type Coordinator struct {
	// Your definitions here.
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask
	// TODO: double check if it's ok to use multiple locks for the same map.
	nameToLockMap   map[string]*sync.Mutex
	nameToStatusMap map[string]TaskStatus
}

func (c *Coordinator) GetTaskHandler(req *GetTaskRequest, resp *GetTaskResponse) error {
	taskAssigned := false
	taskAllDone := true
	for _, task := range c.mapTasks {
		name := task.Task.Name
		c.nameToLockMap[name].Lock()
		if c.nameToStatusMap[name] != SUCCESS {
			taskAllDone = false
		}

		if c.nameToStatusMap[name] == NOTSTARTED || c.nameToStatusMap[name] == FAIL {
			taskAssigned = true
			c.nameToStatusMap[name] = ASSIGNED
			resp.TaskArg = task
			c.nameToLockMap[task.Task.Name].Unlock()
			fmt.Printf("task %v is assigned\n", task.Task.ID)
			break
		}
		c.nameToLockMap[task.Task.Name].Unlock()
		resp.STATUS = TASKASSIGNED
	}

	if taskAllDone {
		resp.STATUS = TASKSALLDONE
	} else if !taskAssigned {
		resp.STATUS = TASKNOTREQDY
	}

	return nil
}

func (c *Coordinator) ChangeTaskStatusHandler(req *ChangeTaskStatusRequest, resp *ChangeTaskStatusResponse) error {
	c.nameToLockMap[req.Name].Lock()
	c.nameToStatusMap[req.Name] = req.Status
	fmt.Printf("Task %v status changed to %v\n", req.Name, req.Status)
	c.nameToLockMap[req.Name].Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
//
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
	// TODO : double check this is right.
	return len(c.mapTasks) == 0 && len(c.reduceTasks) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := []*MapTask{}
	reduceTasks := []*ReduceTask{}
	nameToLockMap := map[string]*sync.Mutex{}
	nameToStatusMap := map[string]TaskStatus{}
	for i, file := range files {
		name := fmt.Sprintf("map-task-%v", i)
		nameToLockMap[name] = &sync.Mutex{}
		mapTasks = append(mapTasks, &MapTask{
			FileName: file,
			Task: &Task{
				Name:      name,
				ID:        i,
				ReduceNum: nReduce,
			},
		})
	}

	for i := 0; i < nReduce; i++ {
		fileList := []string{}
		for j := 0; j < len(files); j++ {
			intermediateFileName := fmt.Sprintf("mr-%v-%v.txt", j, i)
			fileList = append(fileList, intermediateFileName)
		}
		name := fmt.Sprintf("reduce-task-%v", i)
		nameToLockMap[name] = &sync.Mutex{}
		reduceTask := &ReduceTask{
			Files: fileList,
			Task: &Task{
				Name:      name,
				ID:        i,
				ReduceNum: nReduce,
			},
		}
		reduceTasks = append(reduceTasks, reduceTask)
	}

	c := Coordinator{
		mapTasks:        mapTasks,
		reduceTasks:     reduceTasks,
		nameToLockMap:   nameToLockMap,
		nameToStatusMap: nameToStatusMap,
	}

	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

	// Your code here.

	c.server()
	return &c
}
