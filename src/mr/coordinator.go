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

type Coordinator struct {
	// Your definitions here.
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask
	// TODO: double check if it's ok to use multiple locks for the same map.
	idToLockMap   map[string]*sync.Mutex
	idToStatusMap map[string]TaskStatus
}

func (c *Coordinator) GetTaskHandler(args *GetTaskRequest, resp *GetTaskResponse) error {
	taskAssigned := false
	for _, task := range c.mapTasks {
		id := task.Task.ID
		c.idToLockMap[id].Lock()
		if c.idToStatusMap[id] == NOTSTARTED || c.idToStatusMap[id] == FAIL {
			taskAssigned = true
			c.idToStatusMap[id] = ASSIGNED
			resp.TaskArg = task
			c.idToLockMap[task.Task.ID].Unlock()
			fmt.Printf("task %v is assigned\n", task.Task.ID)
			break
		}
		c.idToLockMap[task.Task.ID].Unlock()
	}

	if !taskAssigned {
		return fmt.Errorf("No map task assigned!")
	}
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
	idToLockMap := map[string]*sync.Mutex{}
	idToStatusMap := map[string]TaskStatus{}
	for i, file := range files {
		id := fmt.Sprintf("map-task-%v", i)
		idToLockMap[id] = &sync.Mutex{}
		mapTasks = append(mapTasks, &MapTask{
			FileName: file,
			Task: &Task{
				ID:        id,
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
		id := fmt.Sprintf("reduce-task-%v", i)
		idToLockMap[id] = &sync.Mutex{}
		reduceTask := &ReduceTask{
			Files: fileList,
			Task: &Task{
				ID:        id,
				ReduceNum: nReduce,
			},
		}
		reduceTasks = append(reduceTasks, reduceTask)
	}

	c := Coordinator{
		mapTasks:      mapTasks,
		reduceTasks:   reduceTasks,
		idToLockMap:   idToLockMap,
		idToStatusMap: idToStatusMap,
	}

	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

	// Your code here.

	c.server()
	return &c
}
