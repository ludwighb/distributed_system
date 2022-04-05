package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mapTasks []*MapTask
	reduceTasks []*ReduceTask
}

func (c * Coordinator) CoordinatorHandler(args *GetTaskRequest, resp *GetTaskResponse) (error) {
	for  _, mapTask := range c.mapTasks{
		fmt.Printf("map task: %v\n", mapTask)
	}

	for  _, reduceTask := range c.reduceTasks{
		fmt.Printf("reduce task: %v\n", reduceTask)
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
	return len(c.mapTasks) ==0 && len(c.reduceTasks) ==0 
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := []*MapTask{}
	reduceTasks := []*ReduceTask{}
	for _, file := range files{
		mapTasks = append(mapTasks, &MapTask{FileName: file})
	}

	for i:=0; i< nReduce; i++ {
		fileList := []string{}
		for j := 0; j< len(files); j++{
			intermediateFileName := fmt.Sprintf("mr-%02d-%02d.txt", j, i)
			fileList = append(fileList, intermediateFileName)
		}
		reduceTask := &ReduceTask{Files: fileList}
		reduceTasks = append(reduceTasks, reduceTask)
	}

	c := Coordinator{
		mapTasks: mapTasks,
		reduceTasks: reduceTasks,
	}

	// Your code here.


	c.server()
	return &c
}
