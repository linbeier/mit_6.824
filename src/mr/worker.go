package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	var intermediate []KeyValue

	//send an RPC to the coordinator asking for a task
	workerinfo := CallRegister()

	for {
		workinfo := CallAssign()

		if workinfo.TaskType == maptask {
			kva, _ := MapWork(workinfo.FileName, mapf)
			intermediate = append(intermediate, kva...)

			file, err := os.Open("/var/temp/intermediate/mr-" + strconv.Itoa(workerinfo.WorkerName))
			if err != nil {
				fmt.Println(err)
			}
			enc := json.NewEncoder(file)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println(err)
				}
			}
		} else {

		}
		time.Sleep(1)
	}

}

func CallRegister() RegisterReply {

	args := RegisterArgs{}

	reply := RegisterReply{}

	call("Coordinator.Register", &args, &reply)

	return reply
}

func CallAssign() AssignReply {
	args := AssignArgs{}

	reply := AssignReply{}

	call("Coordinator.Assign", &args, &reply)

	return reply
}

func MapWork(filename string, mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	var intermediate []KeyValue

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	return intermediate, nil
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
