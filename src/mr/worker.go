package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	Key    string
	Values []string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string, reduceNumber int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % reduceNumber
}

func groupByKey(keyValueArr []KeyValue) []KeyValues {
	i := 0
	res := []KeyValues{}
	for i < len(keyValueArr) {
		j := i + 1
		for j < len(keyValueArr) && keyValueArr[j].Key == keyValueArr[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, keyValueArr[k].Value)
		}
		res = append(res, KeyValues{
			Key:    keyValueArr[i].Key,
			Values: values,
		})
		i = j
	}
	return res
}

// T
// Map task:
// Reduce task:
// Return status code when finished.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// TODO: why need to register here?
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

	// Your worker implementation here.
	resp := &GetTaskResponse{}
	callSuccess := call("Coordinator.GetTaskHandler", &GetTaskRequest{}, resp)
	// TODO: wait if the task is not ready
	if !callSuccess {
		log.Fatal("Failed to call coordinator")
	}
	fmt.Printf("resp: %v\n", resp)

	if resp.Type == MAPTASK {
		// TODO: why can't i just resp.TaskArg.(*MapTask)??? Isn't interface can either store a copy of struct, or pointer to struct?
		arg, ok := resp.TaskArg.(MapTask)
		if !ok {
			log.Fatalf("Failed to convert map task arg!")
		}
		err := handleMapTask(&arg, mapf)
		if err != nil {

		}
	} else {
		err := handleReduceTask(resp.TaskArg.(*ReduceTask), reducef)
		if err != nil {

		}
	}

}

// TODO: 1. debug why there are only one record in each file
//       2. send response back to coordinator
func handleMapTask(task *MapTask, mapFunc func(string, string) []KeyValue) error {
	fmt.Printf("start handling task : id: %v, filename: %v\n", task.Task.ID, task.FileName)
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	keyValueArr := mapFunc(task.FileName, string(content))
	sort.Sort(ByKey(keyValueArr))
	sortedKeyVaules := groupByKey(keyValueArr)

	i := 0
	for i < len(sortedKeyVaules) {
		j := i + 1
		for j < len(sortedKeyVaules) && ihash(sortedKeyVaules[j].Key, task.Task.ReduceNum) == ihash(sortedKeyVaules[i].Key, task.Task.ReduceNum) {
			j++
		}

		// TODO: name the file correctly
		intermediateFileName := fmt.Sprintf("mr-%v-%v.json", 0, ihash(sortedKeyVaules[i].Key, task.Task.ReduceNum))
		f, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("Failed to create file %v: %v", f, err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for k := i; k < j; k++ {
			enc.Encode(sortedKeyVaules[k])
		}
		i = j

	}
	return nil
}

func handleReduceTask(task *ReduceTask, reduceFunc func(string, []string) string) error {
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
