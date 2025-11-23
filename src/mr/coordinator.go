package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type FileMap struct {
	Filename         string
	assigned         bool
	done             bool
	lastAssignedTime int64
}

type Coordinator struct {
	// Your definitions here.
	mapFileMap    []FileMap
	reduceFileMap []FileMap

	NReduce int
	mutex   sync.Mutex
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) AllTaskFinished(TaskType TaskType) bool {
	var fileMap []FileMap = c.mapFileMap
	if TaskType == Reduce {
		fileMap = c.reduceFileMap
	}

	for _, file := range fileMap {
		if !file.done {
			return false
		}
	}
	return true
}

func (c *Coordinator) FindUnassignedTask(TaskType TaskType) (index int, task *FileMap, found bool) {
	tasks := c.reduceFileMap
	if TaskType == Map {
		tasks = c.mapFileMap
	}

	for index := range tasks {
		task := &tasks[index]
		if !task.assigned && !task.done {
			return index, task, true
		}
	}
	return -1, nil, false
}

func (c *Coordinator) CheckTaskTimeout() {
	for index := range c.mapFileMap {
		task := &c.mapFileMap[index]
		if task.assigned && !task.done {
			if time.Now().Unix()-task.lastAssignedTime > 10 {
				task.assigned = false
			}
		}
	}

	for index := range c.reduceFileMap {
		task := &c.reduceFileMap[index]
		if task.assigned && !task.done {
			if time.Now().Unix()-task.lastAssignedTime > 10 {
				task.assigned = false
			}
		}
	}
}

func (c *Coordinator) GetTask(req *ReqRespNone, resp *RespTask) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.CheckTaskTimeout()

	var TaskType TaskType
	if !c.AllTaskFinished(Map) {
		TaskType = Map
	} else if !c.AllTaskFinished(Reduce) {
		TaskType = Reduce
	} else {
		*resp = RespTask{TaskType: Exit}
		return nil
	}

	index, task, found := c.FindUnassignedTask(TaskType)
	if !found {
		*resp = RespTask{TaskType: Wait}
		return nil
	}
	task.assigned = true
	task.lastAssignedTime = time.Now().Unix()
	*resp = RespTask{TaskType: TaskType, Filename: task.Filename, NReduce: c.NReduce, TaskID: index, NMap: len(c.mapFileMap)}
	return nil
}

func (c *Coordinator) ReportTaskCompletion(req *ReqFinished, resp *ReqRespNone) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch req.TaskType {
	case Map:
		c.mapFileMap[req.TaskID].assigned = false
		c.mapFileMap[req.TaskID].done = true
	case Reduce:
		c.reduceFileMap[req.TaskID].assigned = false
		c.reduceFileMap[req.TaskID].done = true
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.AllTaskFinished(Reduce)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, NReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, Filename := range files {
		c.mapFileMap = append(c.mapFileMap, FileMap{Filename: Filename, assigned: false, done: false})
	}

	for i := range NReduce {
		reduceFilename := string(rune(i))
		c.reduceFileMap = append(c.reduceFileMap, FileMap{Filename: reduceFilename, assigned: false, done: false})
	}

	c.NReduce = NReduce

	c.server()
	return &c
}
