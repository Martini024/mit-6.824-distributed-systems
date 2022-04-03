package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
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

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func writeIntermidiateFile(kva []KeyValue, nMap int, nReduce int) {
	var regions = make([][]KeyValue, nReduce)
	for _, kv := range kva {
		regionIndex := ihash(kv.Key) % nReduce
		regions[regionIndex] = append(regions[regionIndex], kv)
	}
	for i, region := range regions {
		oname := fmt.Sprint("mr-", nMap, "-", i)
		jsonRegion, _ := json.Marshal(region)
		err := ioutil.WriteFile(oname, jsonRegion, 0644)
		if err != nil {
			fmt.Println(err)
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	// 1. Request for task every 50ms
	for {
		requestForTaskReply := new(RequestForTaskReply)
		ok := call("Coordinator.RequestForTaskHandler", struct{}{}, &requestForTaskReply)
		if ok {
			// 2. Perform task
			if requestForTaskReply.Files != nil {
				jsonReply, _ := json.Marshal(requestForTaskReply)
				fmt.Println(string(jsonReply))
				println(requestForTaskReply == nil)

				switch requestForTaskReply.Type {
				case Map:
					contents := readFile(requestForTaskReply.Files[0])
					kva := mapf(requestForTaskReply.Files[0], contents)
					writeIntermidiateFile(kva, requestForTaskReply.Id, requestForTaskReply.Region)
				}
			} else {
				println("No task assigned but job not done, keep requesting")
			}
		} else {
			// 3. No task assigned, terminate
			println("Terminate because master replies error")
			break
		}
		time.Sleep(1 * time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
