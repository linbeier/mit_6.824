package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
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

	var workinfo AssignReply
	var callch = make(chan AssignReply)

	//dir, err := ioutil.TempDir("", "intermediate")
	//if err != nil {
	//	fmt.Println(err)
	//}
	//os.Rename(dir, "/tmp/intermediate")

	//send an RPC to the coordinator asking for a task
	go CallAssign(callch)

	// every task pair creates a file, and rename by coordinator when all task finished
	for workinfo = range callch {

		switch workinfo.T.TaskType {
		case maptask:

			var intermediate []KeyValue
			var tempfiles []*os.File
			var encoders []*json.Encoder

			for i := 0; i < workinfo.T.NReduce; i++ {

				file, err := ioutil.TempFile(tempdir, "undone-mr-"+strconv.Itoa(workinfo.T.TaskNum)+"-"+strconv.Itoa(i)+"-")
				if err != nil {
					fmt.Println(err)
				}

				encoders = append(encoders, json.NewEncoder(file))
				tempfiles = append(tempfiles, file)
			}

			kva, _ := MapWork(workinfo.T.FileName, mapf)
			intermediate = append(intermediate, kva...)

			for _, kv := range intermediate {
				err := encoders[ihash(kv.Key)%workinfo.T.NReduce].Encode(&kv)
				if err != nil {
					fmt.Println(err)
				}
			}

			for i := 0; i < workinfo.T.NReduce; i++ {
				tempfiles[i].Close()
			}

			for _, file := range tempfiles {
				err := os.Rename(file.Name(), tempdir+"/"+file.Name()[25:])
				if err != nil {
					fmt.Println(err)
				}
			}

		case reducetask:
			var filenames []string

			regstring := "^mr-.-" + strconv.Itoa(workinfo.T.TaskNum) + "-"
			Dirfiles, err := ioutil.ReadDir(tempdir)
			if err != nil {
				fmt.Println(err)
			}
			for _, file := range Dirfiles {
				filenames = append(filenames, file.Name())
			}
			MatchedName := RegPrefixMatch(regstring, filenames)

			var kva map[string][]string
			kva = make(map[string][]string)

			for _, name := range MatchedName {

				file, err := os.Open(tempdir + "/" + name)
				if err != nil {
					fmt.Println(err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}

					kva[kv.Key] = append(kva[kv.Key], kv.Value)
				}
				file.Close()
			}
			ofile, _ := os.Create("mr-out-" + strconv.Itoa(workinfo.T.TaskNum))
			for k, v := range kva {
				fmt.Fprintf(ofile, "%v %v\n", k, reducef(k, v))
			}
			ofile.Close()

		case idle:
			time.Sleep(1 * time.Second)

		}

		CallFinish(workinfo.T.TaskType, workinfo.T.TaskNum)

		time.Sleep(1 * time.Second)

		go CallAssign(callch)
	}
	close(callch)
}

func RegPrefixMatch(regstring string, filenames []string) (reply []string) {
	for _, name := range filenames {
		Matched, err := regexp.MatchString(regstring, name)
		if err != nil {
			fmt.Println(err)
		}
		if Matched {
			reply = append(reply, name)
		}
	}
	return
}

func CallAssign(ReplyChan chan AssignReply) {

	args := AssignArgs{}

	reply := AssignReply{}

	call("Coordinator.Assign", &args, &reply)

	ReplyChan <- reply
}

//todo: call finish
func CallFinish(Tasktype, Tasknum int) {

	args := FinishArgs{
		TaskType: Tasktype,
		TaskNum:  Tasknum,
	}

	reply := FinishReply{}

	call("Coordinator.WorkFinish", &args, &reply)
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
	defer file.Close()
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
