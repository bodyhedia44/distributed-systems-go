package mr

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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

var coordSockName string

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {
		reply := TaskReply{}
		if !CallTask(&TaskArgs{}, &reply) {
			os.Exit(0)
		}
		switch reply.TaskType {
		case ExitTask:
			return
		case WaitTask:
			// The coordinator blocks until work exists, so this is only a fallback.
			time.Sleep(time.Second)
			continue
		}
		if err := processTask(reply, mapf, reducef); err != nil {
			log.Printf("%v task %d failed: %v", reply.TaskType, reply.TaskID, err)
			continue
		}
		ok := CallReplTask(reply.TaskID, reply.TaskType)
		fmt.Println("Report task completion status:", ok)
	}

}

func processTask(t TaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	switch t.TaskType {
	case MapTask:
		return processMapTask(t, mapf)
	case ReduceTask:
		return processReduceTask(t, reducef)
	default:
		return fmt.Errorf("unexpected task type %v", t.TaskType)
	}
}

func processMapTask(t TaskReply, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(t.FileName)
	if err != nil {
		return fmt.Errorf("read input: %w", err)
	}

	kva := mapf(t.FileName, string(content))

	tempFiles := make([]*os.File, t.NReduce)
	writers := make([]*bufio.Writer, t.NReduce)
	encoders := make([]*json.Encoder, t.NReduce)
	defer func() {
		// close anything left open by an early return
		for _, f := range tempFiles {
			if f != nil {
				f.Close()
			}
		}
	}()

	for i := 0; i < t.NReduce; i++ {
		tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-map-temp-%d-*", t.TaskID))
		if err != nil {
			return fmt.Errorf("create temp file: %w", err)
		}
		tempFiles[i] = tempFile
		// buffer so each Encode isn't its own write syscall
		writers[i] = bufio.NewWriter(tempFile)
		encoders[i] = json.NewEncoder(writers[i])
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % t.NReduce
		if err := encoders[bucket].Encode(&kv); err != nil {
			return fmt.Errorf("encode kv: %w", err)
		}
	}

	for i := 0; i < t.NReduce; i++ {
		if err := writers[i].Flush(); err != nil {
			return fmt.Errorf("flush %s: %w", tempFiles[i].Name(), err)
		}
		if err := tempFiles[i].Close(); err != nil {
			return fmt.Errorf("close %s: %w", tempFiles[i].Name(), err)
		}

		finalName := fmt.Sprintf("mr-%d-%d", t.TaskID, i)
		if err := os.Rename(tempFiles[i].Name(), finalName); err != nil {
			return fmt.Errorf("rename temp file to %v: %w", finalName, err)
		}
	}
	return nil
}

// readIntermediate decodes every KeyValue in one map output file.
func readIntermediate(name string) ([]KeyValue, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if errors.Is(err, io.EOF) {
				return kva, nil
			}
			return nil, fmt.Errorf("decode %s: %w", name, err)
		}
		kva = append(kva, kv)
	}
}

func processReduceTask(t TaskReply, reducef func(string, []string) string) error {
	var kva []KeyValue
	for m := 0; m < t.NMap; m++ {
		pairs, err := readIntermediate(fmt.Sprintf("mr-%d-%d", m, t.TaskID))
		if err != nil {
			return err
		}
		kva = append(kva, pairs...)
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-out-temp-%d-*", t.TaskID))
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer tempFile.Close()
	out := bufio.NewWriter(tempFile)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		if _, err := fmt.Fprintf(out, "%v %v\n", kva[i].Key, output); err != nil {
			return fmt.Errorf("write output: %w", err)
		}

		i = j
	}

	if err := out.Flush(); err != nil {
		return fmt.Errorf("flush %s: %w", tempFile.Name(), err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tempFile.Name(), err)
	}

	finalName := fmt.Sprintf("mr-out-%d", t.TaskID)
	if err := os.Rename(tempFile.Name(), finalName); err != nil {
		return fmt.Errorf("rename temp file to %v: %w", finalName, err)
	}
	return nil
}

func CallTask(args *TaskArgs, reply *TaskReply) bool {
	return call("Coordinator.Task", args, reply)
}

func CallReplTask(taskID int, taskType TaskType) bool {
	args := ReportTaskArgs{}

	// fill in the argument(s).
	args.TaskID = taskID
	args.TaskType = taskType
	// declare a reply structure.
	reply := ReportTaskReply{}
	ok := call("Coordinator.TaskReply", &args, &reply)

	return ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
