package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskState int

const (
	idle taskState = iota
	inProgress
	completed
)

const taskTimeout = 10 * time.Second

type task struct {
	id        int
	fileName  string // map tasks only
	state     taskState
	startTime time.Time
}

type Coordinator struct {
	nReduce     int
	mapTasks    []task
	reduceTasks []task
	mu          sync.Mutex
	cond        *sync.Cond // signalled when a task may have become idle or the job finished
}

func allCompleted(tasks []task) bool {
	for i := range tasks {
		if tasks[i].state != completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) isMapPhase() bool {
	return !allCompleted(c.mapTasks)
}

// currentTasks returns the tasks of the active phase and their type.
func (c *Coordinator) currentTasks() ([]task, TaskType) {
	if c.isMapPhase() {
		return c.mapTasks, MapTask
	}
	return c.reduceTasks, ReduceTask
}

// Your code here -- RPC handlers for the worker to call.

func firstIdle(tasks []task) *task {
	for i := range tasks {
		if tasks[i].state == idle {
			return &tasks[i]
		}
	}
	return nil
}

// Task blocks until a task is available or the job is done, so idle
// workers wait inside the RPC instead of polling.
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if allCompleted(c.reduceTasks) {
			reply.TaskType = ExitTask
			return nil
		}
		tasks, taskType := c.currentTasks()
		if t := firstIdle(tasks); t != nil {
			t.state = inProgress
			t.startTime = time.Now()
			reply.TaskType = taskType
			reply.TaskID = t.id
			reply.FileName = t.fileName
			reply.NMap = len(c.mapTasks)
			reply.NReduce = c.nReduce
			log.Printf("Assigned %v task %d", taskType, t.id)
			return nil
		}
		c.cond.Wait()
	}
}

func (c *Coordinator) TaskReply(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var tasks []task
	switch args.TaskType {
	case MapTask:
		tasks = c.mapTasks
	case ReduceTask:
		tasks = c.reduceTasks
	default:
		return fmt.Errorf("unknown task type %v", args.TaskType)
	}
	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return fmt.Errorf("invalid %v task id %d", args.TaskType, args.TaskID)
	}
	tasks[args.TaskID].state = completed
	log.Printf("Reported completion of %v task %d", args.TaskType, args.TaskID)
	// A completion only unblocks waiters when it ends a phase:
	// the last map releases the reduces, the last reduce ends the job.
	if allCompleted(tasks) {
		c.cond.Broadcast()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return allCompleted(c.reduceTasks)
}

func (c *Coordinator) watchfortimeouts() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		tasks, taskType := c.currentTasks()
		for i := range tasks {
			t := &tasks[i]
			if t.state == inProgress && time.Since(t.startTime) > taskTimeout {
				t.state = idle
				log.Printf("%v task %d timed out, resetting to idle", taskType, t.id)
				c.cond.Broadcast()
			}
		}
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := &Coordinator{nReduce: nReduce}
	c.cond = sync.NewCond(&c.mu)

	c.mapTasks = make([]task, len(files))
	for i, file := range files {
		c.mapTasks[i] = task{id: i, fileName: file}
	}

	c.reduceTasks = make([]task, nReduce)
	for i := range c.reduceTasks {
		c.reduceTasks[i].id = i
	}

	go c.watchfortimeouts()
	c.server(sockname)
	return c
}
