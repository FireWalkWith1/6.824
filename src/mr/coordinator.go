package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	nMap       int // 文件数量
	mapTask    []*MapTask
	nReduce    int
	reduceTask []*ReduceTask
	mapDone    bool
	done       bool
	sequence   int
}

type Task struct {
	Executed bool
	Stoped   bool
	Succeed  bool
	Start    time.Time
	Index    int
}

type MapTask struct {
	Task
	SourceFileName string
	OutFileNames   []string
}

type ReduceTask struct {
	Task
	SourceFileNames []string
	OutFileName     string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReqTask(args *TaskArgs, reply *TaskReply) error {
	// log.Println("ReqTask...")
	c.mu.Lock()
	defer c.mu.Unlock()
	// log.Println("ReqTask Lock...")
	if c.done {
		reply.Continue = false
		return nil
	}
	if !c.mapDone {
		for _, task := range c.mapTask {
			if !task.Executed || (task.Stoped && !task.Succeed) {
				reply.Continue = true
				reply.Work = true
				reply.TaskType = "map"
				reply.MapTask = task
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				task.Start = time.Now()
				task.Executed = true
				task.Stoped = false
				// log.Printf("reply=%v", reply)
				// log.Printf("reply.MapTask=%v", reply.MapTask)
				return nil
			}
		}
	} else {
		for _, task := range c.reduceTask {
			if !task.Executed || (task.Stoped && !task.Succeed) {
				reply.Continue = true
				reply.Work = true
				reply.TaskType = "reduce"
				reply.ReduceTask = task
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				task.Start = time.Now()
				task.Executed = true
				task.Stoped = false
				// log.Printf("reply=%v", reply)
				// log.Printf("reply.ReduceTask=%v", reply.ReduceTask)
				return nil
			}
		}
	}
	reply.Continue = true
	reply.Work = false
	// log.Printf("reply=%v", reply)
	// log.Printf("reply.ReduceTask=%v", reply.ReduceTask)
	return nil
}

func (c *Coordinator) SendResult(args *ResultArgs, reply *ResultReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == "map" && !c.mapDone {
		mapTask := c.mapTask[args.Index]
		if mapTask.Executed && !mapTask.Stoped {
			mapTask.Stoped = true
			mapTask.Succeed = args.Result
			if mapTask.Succeed {
				c.setDown()
			}
		}
	} else if args.TaskType == "reduce" && c.mapDone && !c.done {
		reduceTask := c.reduceTask[args.Index]
		if reduceTask.Executed && !reduceTask.Stoped {
			reduceTask.Stoped = true
			reduceTask.Succeed = args.Result
			if reduceTask.Succeed {
				c.setDown()
			}
		}
	}
	return nil
}

func (c *Coordinator) checkTaskTimeOut() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		now := time.Now()
		if !c.done {
			if !c.mapDone {
				for _, mapTask := range c.mapTask {
					if mapTask.Executed && !mapTask.Stoped && now.Sub(mapTask.Start).Seconds() > 10 {
						mapTask.Stoped = true
						mapTask.Succeed = false
					}
				}
			} else {
				for _, reduceTask := range c.reduceTask {
					if reduceTask.Executed && !reduceTask.Stoped && now.Sub(reduceTask.Start).Seconds() > 10 {
						reduceTask.Stoped = true
						reduceTask.Succeed = false
					}
				}
			}
		}
		c.mu.Unlock()
	}

}

func (c *Coordinator) setDown() {
	if !c.done {
		if !c.mapDone {
			mapDone := true
			for _, mapTask := range c.mapTask {
				if !mapTask.Succeed {
					mapDone = false
					break
				}
			}
			if mapDone {
				c.mapDone = true
			}
		} else {
			done := true
			for _, reduceTask := range c.reduceTask {
				if !reduceTask.Succeed {
					done = false
					break
				}
			}
			if done {
				c.done = true
			}
		}
	}
	// log.Printf("c.mapDone:%v", c.mapDone)
	// log.Printf("c.done:%v", c.done)
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.done
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// init c
	c.mapDone = false
	c.nMap = len(files)
	c.mapTask = make([]*MapTask, c.nMap)
	for i, fileName := range files {
		mapTask := new(MapTask)
		mapTask.Index = i
		mapTask.SourceFileName = fileName
		mapTask.OutFileNames = make([]string, nReduce)
		for j, _ := range mapTask.OutFileNames {
			mapTask.OutFileNames[j] = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
		}
		c.mapTask[i] = mapTask
	}

	c.nReduce = nReduce
	c.reduceTask = make([]*ReduceTask, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		reduceTask := new(ReduceTask)
		reduceTask.Index = i
		reduceTask.SourceFileNames = make([]string, c.nMap)
		for j, _ := range reduceTask.SourceFileNames {
			reduceTask.SourceFileNames[j] = "mr-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
		}
		reduceTask.OutFileName = "mr-out-" + strconv.Itoa(i)
		c.reduceTask[i] = reduceTask
	}
	c.sequence = 1
	c.mapDone = false
	c.done = false
	c.server()
	go c.checkTaskTimeOut()
	return &c
}
