package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 双向链表
type Node struct {
	prev *Node
	next *Node
	key int
	files []string
	beginTime time.Time
}

type List struct {
	head *Node
}

type Coordinator struct {
    // num of map task
	mapTaskNum int
	// num of reduce task
	reduceTaskNum int

	nReduce int
	// map task data structure
	mapTaskList *List
	mapTaskDoingMap map[int]*Node
	mapTaskDoingList *List
	mapTaskDoneMap map[int]int
	// reduce task data structure
	reduceTaskList *List
	reduceTaskMap map[int]*Node
	reduceTaskDoingMap map[int]*Node
	reduceTaskDoingList *List
	reduceTaskDoneMap map[int]int
	// lock
	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RpcHandler(args *Args, reply *Reply) error {

	if args.RequestType == 0 {
    	// 水磨功夫
    	// 先不考虑
		// 为什么获取不到锁呢
		c.lock.Lock()
		defer c.lock.Unlock()
		target := c.mapTaskList.head.next
		//fmt.Fprintf(os.Stderr, "第一个mapTask的filename : " + target.files[0] + "\n")
    	reduceTarget := c.reduceTaskList.head.next
    	if target != c.mapTaskList.head {

			reply.TaskType = 0
    		reply.TaskNumber = target.key
    		reply.ReduceFiles = target.files
    		reply.ReduceNum = c.nReduce
    		target.beginTime = time.Now()
    		move(target, c.mapTaskDoingList)
    		c.mapTaskDoingMap[target.key] = target
		} else if c.mapTaskNum != 0 {
			reply.TaskType = 0
			reply.Message = "Please wait!"
		} else if reduceTarget != c.reduceTaskList.head {
			reply.TaskType = 1
			reply.TaskNumber = reduceTarget.key
			reply.ReduceFiles = reduceTarget.files
			reduceTarget.beginTime = time.Now()
			move(reduceTarget, c.reduceTaskDoingList)
			c.reduceTaskDoingMap[reduceTarget.key] = reduceTarget
		} else if c.reduceTaskNum != 0 {
			reply.TaskType = 1
			reply.Message = "Please wait!"
		} else {
			// job is done
			reply.Message = "job is done!"
		}
		//fmt.Fprintf(os.Stderr, "完成分配 : " + target.files[0] + "\n")
		//c.lock.Unlock()
		return nil

	} else {
		c.lock.Lock()
		defer c.lock.Unlock()

		if args.TaskType == 0 {

			// 怎么执行的，唯一标示
			mapTask := c.mapTaskDoingMap[args.TaskNumber]
			if mapTask != nil {
				fmt.Printf("map 任务时间差 %v\n", time.Now().Sub(mapTask.beginTime))
				fmt.Printf("time.second %v\n", time.Second*10)
				if time.Now().Sub(mapTask.beginTime) > (time.Second*10) {
					fmt.Fprintf(os.Stderr, "map任务超时:" + "\n")
					move(mapTask, c.mapTaskList)
					delete(c.mapTaskDoingMap, mapTask.key)
				} else {

					deleteNode(mapTask, c.mapTaskDoingList)
					delete(c.mapTaskDoingMap, mapTask.key)
					c.mapTaskDoneMap[mapTask.key] = mapTask.key
					c.mapTaskNum -= 1
					fmt.Printf("c.mapTaskNum doing %v\n", c.mapTaskNum)

					// 组建reduce task

					for i := 0; i < len(args.IntermediateFile); i++ {

						filename := args.IntermediateFile[i]
						position := strings.LastIndexByte(filename, '-')
						numStr := filename[position + 1:]
						num, _ := strconv.Atoi(numStr)

						// 还是需要一个map
						c.reduceTaskMap[num].files = append(c.reduceTaskMap[num].files, filename)
					}
				}
			}
		} else {
			reduceTask := c.reduceTaskDoingMap[args.TaskNumber]
			if reduceTask != nil {
				fmt.Printf("reduce 任务时间差 %v\n", time.Now().Sub(reduceTask.beginTime))

				if time.Now().Sub(reduceTask.beginTime) > (time.Second*10) {
					fmt.Fprintf(os.Stderr, "reduce任务超时:" + "\n")
					move(reduceTask, c.reduceTaskList)
					delete(c.reduceTaskDoingMap, reduceTask.key)
				} else {
					// 删除
					deleteNode(reduceTask, c.reduceTaskDoingList)
					delete(c.reduceTaskDoingMap, reduceTask.key)
					c.mapTaskDoneMap[reduceTask.key] = reduceTask.key
					c.reduceTaskNum -= 1
				}
			}
		}
	}
    return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	fmt.Printf("done %v\n", "在等待呢。。。。")
	c.lock.Lock()
	defer c.lock.Unlock()
	fmt.Printf("c.mapTaskNum %v\n", c.mapTaskNum)
	fmt.Printf("c.reduceTaskNum %v\n", c.reduceTaskNum)

	return c.reduceTaskNum == 0 && c.mapTaskNum == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}

	c.mapTaskNum = len(files)
	c.reduceTaskNum = nReduce
    c.nReduce = nReduce
    // mapTaskList
    c.mapTaskList = initList()
    i := len(files) - 1
    for i >= 0 {
    	node := Node{}
    	node.key = i
		node.files = append(node.files, files[i])
		addFirst(&node, c.mapTaskList)
		i -= 1
	}
	// mapTaskDoingList
	c.mapTaskDoingList = initList()
	// mapTaskDoingMap
	c.mapTaskDoingMap = map[int]*Node{}
	// mapTaskDoneMap
	c.mapTaskDoneMap = map[int]int{}

	// reduceTaskList
	c.reduceTaskList = initList()
	// reduceTaskMap
	c.reduceTaskMap = map[int]*Node{}
	j := 0
	for j < nReduce {
		node := Node{}
		node.key = j
		addFirst(&node, c.reduceTaskList)
		c.reduceTaskMap[j] = &node
		j += 1
	}

	// reduceTaskDoingList
	c.reduceTaskDoingList = initList()
	// reduceTaskDoingMap
	c.reduceTaskDoingMap = map[int]*Node{}
	// reduceTaskDoneMap
	c.mapTaskDoneMap = map[int]int{}

	c.server()
	c.Evict()
	return &c
}

func initList() *List{
	list := List{}
	sentinel := Node{}
	list.head = &sentinel
	list.head.next = list.head
	list.head.prev = list.head
	return &list
}

func addFirst(n *Node, l *List) {
	n.next = l.head.next
	l.head.next.prev = n
	n.prev = l.head
	l.head.next = n
}

func move(n *Node, l *List) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
	addFirst(n, l)
}

func deleteNode(n *Node, l *List) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
}

// 多长时间循环一次，1s
func (c *Coordinator) Evict() {
	for {
		c.lock.Lock()

		if c.mapTaskNum != 0 {
			i := c.mapTaskDoingList.head.next
			for i != c.mapTaskDoingList.head {
				if time.Now().Sub(i.beginTime) > (time.Second*10) {
					tmp := i.next
					move(i, c.mapTaskList)
					delete(c.mapTaskDoingMap, i.key)
					i = tmp
				} else {
					i = i.next
				}
			}
		} else if c.reduceTaskNum != 0 {
			j := c.reduceTaskDoingList.head.next
			for j != c.reduceTaskDoingList.head {
				if time.Now().Sub(j.beginTime) > (time.Second*10) {
					tmp := j.next
					move(j, c.reduceTaskList)
					delete(c.reduceTaskDoingMap, j.key)
					j = tmp
				} else {
					j = j.next
				}
			}
		} else {
			fmt.Printf("evit %v\n", "不是放吗。。。。")
			c.lock.Unlock()
			break
		}
		c.lock.Unlock()
		fmt.Printf("evit %v\n", "是放了。。。。")

		time.Sleep(time.Second)
	}

}
