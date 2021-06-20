package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Printf("worker 启动: %v\n", time.Now())


	// 最外层是一个循环体
	for {
		//循环发送请求，中间间隔时间
		rpcname := "Coordinator.RpcHandler"
		args := Args{}
		args.RequestType = 0
		reply := Reply{}

		fmt.Printf("worker在执行: %v\n", time.Now())

		call(rpcname, &args, &reply)
		fmt.Printf("worker message: %v\n", reply.Message)

		if reply.Message == "Please wait!"{
			time.Sleep(time.Second)
			continue
		}

		if reply.Message == "job is done!"{
			break
		}
		// 只有一种解释，coordinate执行完任务，结束了
		/*if err != nil {
			fmt.Fprintf(os.Stderr, "Usage: here xxx.so\n")
			break
		}*/
		// reply 获得两个参数，文件名，map还是reduce
		files := []string{}

		if reply.TaskType == 0 {
			// map task
			//fmt.Fprintf(os.Stderr, "files:" + strconv.Itoa(len(reply.ReduceFiles))+  "\n")
			// 为什么会分配一个0的
			//fmt.Fprintf(os.Stderr, "message:" + reply.Message +  "\n")
			files = mapTask(reply.ReduceFiles[0], reply.TaskNumber, reply.ReduceNum, mapf)
		} else {
			// reduce task
			reduceTask(reply.ReduceFiles, reply.TaskNumber, reducef)
		}

		// 继续调用
		args.RequestType = 1
		args.TaskNumber = reply.TaskNumber
		args.TaskType = reply.TaskType
		if reply.TaskType == 0 {
			args.IntermediateFile = files
		}
		call(rpcname, &args, &reply)

		fmt.Printf("worker在执行最后: %v\n", time.Now())

		// 暂停一会儿
		time.Sleep(time.Second)
	}

	fmt.Printf("worker message: %v\n", "先走一步！！！")

}

func mapTask(filename string, mapNum int, nReduce int, mapf func(string, string) []KeyValue) []string {

	strings := []string{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	intermediate := mapf(filename, string(content))

	// 先将所有文件创建好，在这个里面没什么问题
	files := make([]*os.File, 0, nReduce)
	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	for r := 0; r < nReduce; r++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatal("cannot open tmpfile")
		}
		tmpFilename := tmpFile.Name()

		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilenames = append(tmpFilenames, tmpFilename)

	}

	for i := 0; i < len(intermediate); i++ {
		reduceNum := ihash(intermediate[i].Key) % nReduce

		enc := json.NewEncoder(tmpFiles[reduceNum])
		err1 := enc.Encode(intermediate[i])

		if err1 != nil {
			log.Fatalf("cannot encode %v", files[reduceNum])
		}
	}

	for j := 0; j < len(tmpFiles); j++ {
		tmpFiles[j].Close()
	}

	for r := 0; r < nReduce; r++ {
		intermediateFile := "mr-" + strconv.Itoa(mapNum) + "-" + strconv.Itoa(r)
        os.Rename(tmpFilenames[r], intermediateFile)
		strings = append(strings, intermediateFile)
	}
	return strings
}

func reduceTask(filename []string, reduceNum int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, filename := range filename {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatal("cannot open tmpfile")
	}
	tmpFilename := tmpFile.Name()

	oname := "mr-out-" + strconv.Itoa(reduceNum)

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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpFile.Close()
	os.Rename(tmpFilename, oname)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
	// 不要有内部投资，搞笑呢你
	// 这是个什么问题呢？
	//
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
