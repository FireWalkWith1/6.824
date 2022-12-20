package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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
	Key   string
	Value []string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// log.Printf("worker start...")
	// Your worker implementation here.
	for {
		reply := CallTask()
		if !reply.Continue {
			os.Exit(0)
		}
		if !reply.Work {
			// time.Sleep(time.Duration(time.Millisecond * 100))
			time.Sleep(time.Second)
			continue
		}
		if reply.TaskType == "map" {
			handleMap(reply, mapf)
		} else if reply.TaskType == "reduce" {
			handleReduce(reply, reducef)
		}
	}

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// for sorting by key.
type ByKeyForKeyValues []KeyValues

// for sorting by key.
func (a ByKeyForKeyValues) Len() int           { return len(a) }
func (a ByKeyForKeyValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKeyForKeyValues) Less(i, j int) bool { return a[i].Key < a[j].Key }

func handleMap(reply *TaskReply, mapf func(string, string) []KeyValue) {
	// log.Printf("handleMap start...")
	mapTask := reply.MapTask
	file, err := os.Open(mapTask.SourceFileName)
	if err != nil {
		SendResult(reply.TaskType, mapTask.Index, false)
		log.Fatalf("cannot open %v", mapTask.SourceFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		SendResult(reply.TaskType, mapTask.Index, false)
		log.Fatalf("cannot read %v", mapTask.SourceFileName)
	}
	file.Close()
	kva := mapf(mapTask.SourceFileName, string(content))

	kvaa := make([][]KeyValue, reply.NReduce)
	for i, _ := range kvaa {
		kvaa[i] = []KeyValue{}
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NReduce
		ikva := kvaa[index]
		ikva = append(ikva, kv)
		// 这里再关注一下
		kvaa[index] = ikva
	}

	for _, ikva := range kvaa {
		sort.Sort(ByKey(ikva))
	}

	for index, fileName := range mapTask.OutFileNames {
		tempFile, err := ioutil.TempFile("./", fileName+"*")
		defer tempFile.Close()
		if err != nil {
			SendResult(reply.TaskType, reply.MapTask.Index, false)
			log.Fatalf("cannot create tmp file %v", fileName+"*")
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvaa[index] {
			err := enc.Encode(&kv)
			if err != nil {
				SendResult(reply.TaskType, reply.MapTask.Index, false)
				log.Fatalf("cannot encode tmp file %v", fileName+"*")
			}
		}
		os.Rename(tempFile.Name(), fileName)
	}
	SendResult(reply.TaskType, mapTask.Index, true)

}

func handleReduce(reply *TaskReply, reducef func(string, []string) string) {
	// log.Printf("handleReduce start...")
	reduceTask := reply.ReduceTask

	partResult := []KeyValues{}
	for _, fileName := range reduceTask.SourceFileNames {
		sourcesFile, err := os.Open(fileName)
		defer sourcesFile.Close()
		if err != nil {
			SendResult(reply.TaskType, reduceTask.Index, false)
			log.Fatalf("cannot create open file %v", fileName)
		}
		dec := json.NewDecoder(sourcesFile)
		intermediate := []KeyValue{}
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			partResult = append(partResult, KeyValues{intermediate[i].Key, values})

			i = j
		}
	}
	sort.Sort(ByKeyForKeyValues(partResult))
	result := []KeyValue{}
	i := 0
	for i < len(partResult) {
		j := i + 1
		for j < len(partResult) && partResult[j].Key == partResult[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, partResult[k].Value...)
		}
		output := reducef(partResult[i].Key, values)
		result = append(result, KeyValue{partResult[i].Key, output})

		i = j
	}

	ofile, _ := os.Create(reduceTask.OutFileName)
	for _, kv := range result {
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
	}
	SendResult(reply.TaskType, reduceTask.Index, true)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallTask() *TaskReply {

	// declare an argument structure.
	args := TaskArgs{}

	// declare a reply structure.
	reply := TaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.ReqTask", &args, &reply)

	return &reply
}

func SendResult(taskType string, index int, result bool) *ResultReply {
	args := ResultArgs{taskType, index, result}
	reply := ResultReply{}
	call("Coordinator.SendResult", &args, &reply)
	return &reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
