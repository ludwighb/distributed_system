package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



// T
// Map task: 
// Reduce task:
// Return status code when finished. 
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	resp := &GetTaskResponse{}
	callSuccess := call("Coordinator.CoordinatorHandler", &GetTaskRequest{}, resp)
	// TODO: wait if the task is not ready
	if !callSuccess {
		log.Fatal("Failed to call coordinator")
	}
	fmt.Printf("resp: %v\n", resp)

	if resp.Type == MAPTASK {
		err :=handleMapTask(resp.TaskArg.(*MapTask), mapf)
		if err != nil {
			
		}
	}else{
		// TODO: handle reduce task
	}

}

func handleMapTask(task *MapTask, mapFunc func(string, string) []KeyValue) error{
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	request := GetTaskRequest{}
	reply := GetTaskResponse{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &request, &reply)

	fmt.Printf("reply.Y %v\n", reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// TODO: exit when coordinator exits
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
