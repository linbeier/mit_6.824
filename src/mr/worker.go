package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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
	var tempfiles []*os.File
	var encoders []*json.Encoder
	var tempdir string
	var workinfo AssignReply
	var callch = make(chan AssignReply)

	//todo: change tempfile to file
	dir, err := ioutil.TempDir("", "intermediate")
	if err != nil {
		fmt.Println(err)
	}
	err = os.Rename(dir, "/tmp/intermediate")
	if err != nil {
		fmt.Println(err)
	}
	tempdir = "/tmp/intermediate"

	go CallAssign(callch)

	//send an RPC to the coordinator asking for a task
	for workinfo = range callch {
		func() {
			for i := 0; i < workinfo.t.NReduce; i++ {

				filename := filepath.Join(tempdir, "mr-"+strconv.Itoa(workinfo.t.TaskNum)+"-")
				file, err := ioutil.TempFile(tempdir, filename)
				if err != nil {
					fmt.Println(err)
				}
				defer file.Close()

				encoders = append(encoders, json.NewEncoder(file))
				tempfiles = append(tempfiles, file)
			}
		}()

		switch workinfo.t.TaskType {
		case maptask:
			kva, _ := MapWork(workinfo.t.FileName, mapf)
			intermediate = append(intermediate, kva...)

			for _, kv := range intermediate {
				err = encoders[ihash(kv.Key)].Encode(&kv)
				if err != nil {
					fmt.Println(err)
				}
			}

		case reducetask:

		case idle:

		}
		time.Sleep(10 * time.Second)
	}

}

func CallAssign(ch chan AssignReply) {

	args := AssignArgs{}

	reply := AssignReply{}

	call("Coordinator.Assign", &args, &reply)

	ch <- reply
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
