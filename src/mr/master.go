package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"
const TaskTimeout = 10

type TaskStatus int
type TaskType int
type JobStage int

// 常量枚举
// iota 是 Go 语言的一个常量生成器，它在每个 const 声明块中，从 0 开始逐行递增。
// 每定义一个新常量，iota 的值会自动加 1。
// Task的类型
const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask //"please exit" pseudo-task that the master can give to workers.
)

// 用来检测task的状态，master进行跟踪
const (
	NotStarted TaskStatus = iota
	Executing
	Finished
)

// 一个Task包含的内容
type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int // 一开始master循环创建task的时候分配的index
	File     string
	WorkerId int
}

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.ReduceCount = len(m.reduceTasks)
	return nil
}

// assign task to works
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	// find one task, nMap nRedcue都是已经总数-已经执行完后的数量
	// 已经执行完状态，是worker向master汇报的
	var task *Task
	if m.nMap > 0 {
		task = m.selectTask(m.mapTasks, args.WorkerId)
	} else if m.nReduce > 0 {
		task = m.selectTask(m.reduceTasks, args.WorkerId)
	} else {
		// 没有Task了
		task = &Task{ExitTask, Finished, -1, "", args.WorkerId}
	}
	// 记录这个work的ID（PId）
	// 记录这个job能否在10s内完成
	// 用一个goroutine来waitForTask
	reply.TaskFile = task.File
	reply.TaskId = task.Index
	reply.TaskType = task.Type
	m.mu.Unlock()
	go m.waitForTask(task)

	return nil
}

func (m *Master) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var task *Task
	if args.TaskType == MapTask {
		task = &m.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		task = &m.reduceTasks[args.TaskId]
	}

	// 接下来更新剩余任务数量
	// 判断workId是否相同，是为了防止有的超时worker执行完有进行更新
	if task.WorkerId == args.WorkId && task.Status == Executing {
		task.Status = Finished
		if task.Type == MapTask {
			m.nMap--
		} else if task.Type == ReduceTask {
			m.nReduce--
		}
	}
	reply.CanExit = m.nMap == 0 && m.nReduce == 0
	return nil
}

func (m *Master) selectTask(taskList []Task, WorkerId int) *Task {
	var task *Task
	for i := 0; i < len(taskList); i++ {
		if taskList[i].Status == NotStarted {
			task = &taskList[i]
			task.Status = Executing
			task.WorkerId = WorkerId
			return task
		}
	}
	// 没有找到还未执行的task
	task = &Task{NoTask, Finished, -1, "", WorkerId}
	return task
}

func (m *Master) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		return
	}
	<-time.After(time.Second * TaskTimeout)
	// 阻塞指定时间后，查看task状态
	//TODO 这里是必须的吗
	// m.mu.Lock()
	// defer m.mu.Unlock()

	if task.Status != Finished {
		// failed
		// 修改task指针指向的task的状态，此时m中的list保存的task的状态也改变了
		task.Status = NotStarted
		task.WorkerId = -1
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nMap == 0 && m.nReduce == 0
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	nMap := len(files)
	m.nMap = nMap
	m.nReduce = nReduce
	m.mapTasks = make([]Task, 0, nMap)
	m.reduceTasks = make([]Task, 0, nReduce)
	for i := 0; i < nMap; i++ {
		mTask := Task{MapTask, NotStarted, i, files[i], -1}
		m.mapTasks = append(m.mapTasks, mTask)
	}

	for i := 0; i < nReduce; i++ {
		rTask := Task{ReduceTask, NotStarted, i, "", -1}
		m.reduceTasks = append(m.reduceTasks, rTask)
	}

	m.server()

	// 将之前生成的tmp文件移除掉
	// clean up and create temp directory
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &m
}
