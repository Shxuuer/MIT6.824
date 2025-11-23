package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

func HandleMap(task RespTask, mapf func(string, string) []KeyValue) {
	// log.Printf("Worker: Handling Map Task %d on file %s\n", task.TaskID, task.Filename)
	Filename := task.Filename
	file, err := os.Open(Filename)
	if err != nil {
		log.Fatalf("cannot open %v", Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", Filename)
	}
	file.Close()

	kva := mapf(Filename, string(content))

	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % task.NReduce
		buckets[reduceTaskNumber] = append(buckets[reduceTaskNumber], kv)
	}

	for r := 0; r < task.NReduce; r++ {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskID, r)
		ofile, _ := os.Create(oname)
		for _, kv := range buckets[r] {
			fmt.Fprintf(ofile, "%s %s\n", kv.Key, kv.Value)
		}
	}
}

func HandleReduce(task RespTask, reducef func(string, []string) string) {
	// log.Printf("Worker: Handling Reduce Task %d on file %s\n", task.TaskID, task.Filename)
	intermediate := []KeyValue{}
	for i := range task.NMap {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// os.Remove(filename)
		lines := string(content)
		for _, line := range strings.Split(lines, "\n") {
			if len(line) == 0 {
				continue
			}
			kv := KeyValue{}
			fmt.Sscanf(line, "%s %s", &kv.Key, &kv.Value)
			intermediate = append(intermediate, kv)
		}
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := "mr-out-" + strconv.Itoa(task.TaskID)
	ofile, _ := os.Create(oname)

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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		task := RespTask{}
		ok := call("Coordinator.GetTask", &ReqRespNone{}, &task)
		if !ok || task.TaskType == Exit {
			break
		}
		switch task.TaskType {
		case Map:
			HandleMap(task, mapf)
		case Reduce:
			HandleReduce(task, reducef)
		case Wait:
			time.Sleep(500 * time.Millisecond)
		}
		report := ReqFinished{TaskType: task.TaskType, TaskID: task.TaskID}
		call("Coordinator.ReportTaskCompletion", &report, &ReqRespNone{})
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
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
