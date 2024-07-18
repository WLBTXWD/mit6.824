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
	"sort"
	"time"
)

var nReduce int

const TaskInterval = 200

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	n, succ := getReduceCount()
	if !succ {
		fmt.Println("Failed to get reduce task count, worker exiting.")
		return
	}
	nReduce = n
	fmt.Printf("nReduce: %v\n", nReduce)

	for {
		reply, succ := requestTask()
		if !succ {
			fmt.Println("Failed to contact master, worker exiting.")
			return
		}

		// 根据request得到的TaskType，进行对应的处理
		if reply.TaskType == ExitTask {
			fmt.Println("All tasks are done, worker exiting.")
			return
		}

		exit, succ := false, true
		if reply.TaskType == NoTask {
			// 没有任务可分配，但是也不能结束，因为可能有任务fail了
		} else if reply.TaskType == MapTask {
			doMap(mapf, reply.TaskFile, reply.TaskId)
			exit, succ = ReportTaskDone(MapTask, reply.TaskId)
		} else if reply.TaskType == ReduceTask {
			doReduce(reducef, reply.TaskId)
			exit, succ = ReportTaskDone(ReduceTask, reply.TaskId)
		}

		if exit || !succ {
			fmt.Println("Master exited or all tasks done, worker exiting.")
			return
		}

		// worker need to wait between each request
		time.Sleep(time.Millisecond * TaskInterval)
	}

}

// map将结果通过hash，保存在一个reduce_id文件中 tmp/mr-map_id-reduce_id
// reduce读取mr_map_*-reduce_id 读取对应reduce_id的所有文件
func doMap(mapf func(string, string) []KeyValue, filename string, taskId int) {
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

	// create file
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, taskId)
	files := make([]*os.File, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v", prefix, i) // tmp/mr-map_id-reduce_id
		file, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("Cannot create file %v\n", filePath)
		}
		files = append(files, file)
		encoders = append(encoders, json.NewEncoder(file))
	}

	for _, kv := range kva {
		reduce_id := ihash(kv.Key) % nReduce
		err := encoders[reduce_id].Encode(&kv)
		if err != nil {
			log.Fatal("Cannot encode key %v", kv.Key)
		}
	}
}

func doReduce(reducef func(string, []string) string, reduceId int) {
	// 读取所有tmp/mr-mapid-current_reduceId 文件
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		log.Fatal("Cannot list reduce files\n")
	}
	kvMap := make(map[string][]string) // key是string，[]string是value
	var kv KeyValue
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Cannot open file %v\n", filePath)
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatal("Cannot decode from file %v\n", filePath)
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	// write the result to files
	// sort
	keys := make([]string, 0, len(kvMap))
	for key := range kvMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// create file 每个reduceTask 生成一个file
	// tmp/mr-out-X
	filePath := fmt.Sprintf("mr-out-%v", reduceId)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal("Cannot create file %v\n", filePath)
	}

	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, v)
		if err != nil {
			log.Fatal("Cannot write mr output (%v, %v) to file", k, v)
		}
	}

	file.Close()
	// 没有加partial file rename的处理方式
}

func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	succ := call("Master.GetReduceCount", &args, &reply)

	return reply.ReduceCount, succ
}

func requestTask() (*RequestTaskReply, bool) {
	args := RequestTaskArgs{os.Getpid()}
	reply := RequestTaskReply{}
	succ := call("Master.RequestTask", &args, &reply)
	return &reply, succ
}

func ReportTaskDone(TaskType TaskType, taskId int) (bool, bool) {
	args := ReportTaskArgs{TaskType, os.Getpid(), taskId}
	reply := ReportTaskReply{}
	succ := call("Master.ReportTaskDone", &args, &reply)
	return reply.CanExit, succ
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
