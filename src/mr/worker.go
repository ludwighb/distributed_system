package mr

import (
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
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
type KeyValueByKey []KeyValue

// for sorting by key.
func (a KeyValueByKey) Len() int           { return len(a) }
func (a KeyValueByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyReduce struct {
	Key          string
	ReducedValue string
}

type KeyReduceByKey []KeyReduce

// for sorting by key.
func (a KeyReduceByKey) Len() int           { return len(a) }
func (a KeyReduceByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyReduceByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	waitTime = 5 * time.Second
)

var (
	clearIntermediateFile = flag.Bool("clear_files", true, "clear intermediate files when worker exit.")
)

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

// TODO: handle task fail when finished.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// TODO: why need to register here?
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

	for true {
		resp := &GetTaskResponse{}
		callSuccess := call("Coordinator.GetTaskHandler", &GetTaskRequest{}, resp)
		if !callSuccess {
			log.Fatal("Failed to call coordinator")
			os.Exit(1)
		}
		// TODO: use switch instead.
		if resp.STATUS == TASKSALLDONE {
			fmt.Printf("All tasks finished. Exiting worker\n")
			if *clearIntermediateFile {
				err := clearIntermediateFiles()
				if err != nil {
					log.Fatalf("Failed to clear intermediate files: %v", err)
				}
			}
			return
		} else if resp.STATUS == TASKNOTREAQDY {
			pid := os.Getpid()
			fmt.Printf("Worker process %v waiting for task ready to process\n", pid)
			time.Sleep(waitTime)
			continue
		}

		if resp.Type == MAPTASK {
			// TODO: why can't i just resp.TaskArg.(*MapTask)??? Isn't interface can either store a copy of struct, or pointer to struct?
			arg, ok := resp.TaskArg.(MapTask)
			if !ok {
				log.Fatalf("Failed to convert map task arg!")
			}
			err := handleMapTask(&arg, mapf)
			if err != nil {
				log.Fatalf("HandleMapTask(%v) error: %v", arg, err)
			}
		} else {
			arg, ok := resp.TaskArg.(ReduceTask)
			if !ok {
				log.Fatalf("Failed to convert reduce task arg!")
			}
			err := handleReduceTask(&arg, reducef)
			if err != nil {
				log.Fatalf("HandleReduceTask(%v) error: %v", arg, err)
			}
		}

	}
}

func clearIntermediateFiles() error {
	files, err := filepath.Glob("./mr-*.json")
	if err != nil {
		return err
	}

	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			return err
		}
	}

	return nil
}

// splitKeyValuesToIntermediateFiles reads the combined keyValues and spilt them into nReduce files
// and return a map of intermediate file name to []KeyValues belong to that file.
// it uses ihash(key)% to decide which reduce file to go.
func splitKeyValuesToIntermediateFiles(kvs []*KeyValues, nReduce int, mapTaskID int) map[string][]*KeyValues {
	intermediateFileContents := map[string][]*KeyValues{}

	for _, kv := range kvs {
		reduceTaskNum := ihash(kv.Key) % nReduce
		intermediateFileName := fmt.Sprintf("mr-%v-%v.json", mapTaskID, reduceTaskNum)

		if _, ok := intermediateFileContents[intermediateFileName]; !ok {
			intermediateFileContents[intermediateFileName] = []*KeyValues{}
		}

		intermediateFileContents[intermediateFileName] = append(intermediateFileContents[intermediateFileName], kv)
	}

	return intermediateFileContents
}

// TODO: simplify the code
func handleMapTask(task *MapTask, mapFunc func(string, string) []KeyValue) error {
	// Opening file
	fmt.Printf("start handling map task : id: %v, filename: %v\n", task.Task.ID, task.FileName)
	file, err := os.Open(task.FileName)
	if err != nil {
		return fmt.Errorf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read %v", task.FileName)
	}
	file.Close()

	// sort key and group values by key
	keyValueArr := mapFunc(task.FileName, string(content))
	sort.Sort(KeyValueByKey(keyValueArr))
	sortedKeyVaules := groupByKey(keyValueArr)
	sortedValuePointers := []*KeyValues{}

	for i := 0; i < len(sortedKeyVaules); i++ {
		sortedValuePointers = append(sortedValuePointers, &sortedKeyVaules[i])
	}

	intermediateFileContent := splitKeyValuesToIntermediateFiles(sortedValuePointers, task.Task.ReduceNum, task.Task.ID)

	for fileName, content := range intermediateFileContent {
		f, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("failed to create file %v: %v", f, err)
		}
		defer f.Close()
		encoder := json.NewEncoder(f)
		err = encoder.Encode(content)
		if err != nil {
			log.Fatalf("Faied to encode content %v into file %v: %v", content, fileName, err)
		}
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
	fmt.Printf("start handling reduce task : id: %v, files: %v\n", task.Task.ID, task.Files)
	// 1. read all content from all intermediate files
	keyLists := map[string][]string{}
	for _, file := range task.Files {
		err := readFileIntoMap(file, keyLists)
		if err != nil {
			return fmt.Errorf("failed to read intermediate file %v: %v\n", file, err)
		}
	}

	// 2. process all key , list in memory
	keyReduces := []KeyReduce{}
	for key, values := range keyLists {
		keyReduces = append(keyReduces, KeyReduce{Key: key, ReducedValue: reduceFunc(key, values)})
	}
	// 3. content by key
	sort.Sort(KeyReduceByKey(keyReduces))
	// 4. write to output file
	outputFile, err := os.Create(fmt.Sprintf("mr-out-%v", task.Task.ID))
	if err != nil {
		return err
	}

	for _, kr := range keyReduces {
		fmt.Fprintf(outputFile, "%v %v\n", kr.Key, kr.ReducedValue)
	}

	req := &ChangeTaskStatusRequest{
		Name:   task.Task.Name,
		Status: SUCCESS,
	}
	callSuccess := call("Coordinator.ChangeTaskStatusHandler", req, &ChangeTaskStatusResponse{})
	if !callSuccess {
		return fmt.Errorf("Failed to call change task status!")
	}
	return nil
}

func readFileIntoMap(fileName string, keyListMap map[string][]string) error {
	// Not all map task produce full range of intermediate files
	_, err := os.Stat(fileName)
	if err != nil {
		fmt.Printf("file %v doesn't exit \n", fileName)
		return nil
	}

	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(file)
	kvs := []KeyValues{}
	if err := dec.Decode(&kvs); err != nil {
		return fmt.Errorf("failed to dedoce: %v\n", err)
	}

	for _, kv := range kvs {
		if _, ok := keyListMap[kv.Key]; !ok {
			keyListMap[kv.Key] = kv.Values
		} else {
			keyListMap[kv.Key] = append(keyListMap[kv.Key], kv.Values...)
		}
	}

	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
