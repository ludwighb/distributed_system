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
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
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

	for true {
		resp := &GetTaskResponse{}
		callSuccess := call("Coordinator.GetTaskHandler", &GetTaskRequest{}, resp)
		// TODO: wait if the task is not ready
		if !callSuccess {
			log.Fatal("Failed to call coordinator")
		}
		// TODO: let worker sleep when task not ready
		if resp.STATUS == TASKSALLDONE {
			fmt.Printf("All tasks finished. Exiting worker")
			return
		}

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

}

func handleMapTask(task *MapTask, mapFunc func(string, string) []KeyValue) error {
	fmt.Printf("start handling task : id: %v, filename: %v\n", task.Task.ID, task.FileName)
	file, err := os.Open(task.FileName)
	if err != nil {
		return fmt.Errorf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read %v", task.FileName)
	}
	file.Close()
	keyValueArr := mapFunc(task.FileName, string(content))
	sort.Sort(ByKey(keyValueArr))
	sortedKeyVaules := groupByKey(keyValueArr)

	intermediateFileNames := make([]string, task.Task.ReduceNum)
	files := make([]*os.File, task.Task.ReduceNum)
	encoders := make([]*json.Encoder, task.Task.ReduceNum)
	tempArr := make([][]*KeyValues, task.Task.ReduceNum)

	for i := 0; i < task.Task.ReduceNum; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v.json", task.Task.ID, ihash(sortedKeyVaules[i].Key)%task.Task.ReduceNum)
		intermediateFileNames[i] = intermediateFileName
		f, err := os.Create(intermediateFileName)
		if err != nil {
			return fmt.Errorf("failed to create file %v: %v", f, err)
		}
		files[i] = file
		encoders[i] = json.NewEncoder(f)
		tempArr[i] = []*KeyValues{}
	}

	for i := 0; i < len(sortedKeyVaules); i++ {
		hash := ihash(sortedKeyVaules[i].Key) % task.Task.ReduceNum
		tempArr[hash] = append(tempArr[hash], &sortedKeyVaules[i])
	}

	for i := 0; i < task.Task.ReduceNum; i++ {
		encoders[i].Encode(tempArr[i])
		files[i].Close()
	}

	changeStatusReq := &ChangeTaskStatusRequest{
		Name:   task.Task.Name,
		Status: SUCCESS,
	}
	changeStatusResp := &ChangeTaskStatusResponse{}

	fmt.Printf("map task %v done! \n", task.Task.Name)
	callSuccess := call("Coordinator.ChangeTaskStatusHandler", changeStatusReq, changeStatusResp)
	if !callSuccess {
		return fmt.Errorf("failed to call Coordinator.ChangeTaskStatusHandler!")
	}
	return nil
}

func handleReduceTask(task *ReduceTask, reduceFunc func(string, []string) string) error {
	return nil
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
