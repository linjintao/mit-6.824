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

type SchedulePhase int

type Coordinator struct {
	// Your definitions here.
  files []string
  nReduce int
  nMap    int
  mapTaskLog []int
  mapFinished int
  reduceTaskLog []int
  reduceFinished int
  mu sync.Mutex
}

type TaskStatus int

type Task struct {
  fileName string
  id       int
  startTime time.Time
  status    TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) MapFinished(args *WorkerArgs, reply *WorkerReply) error {
  c.mu.Lock()
  c.mapFinished++
  c.mapTaskLog[args.MapTaskNumber] = 2
  c.mu.Unlock()
  return nil
}

func (c *Coordinator) ReduceFinished(args *WorkerArgs, reply *WorkerReply) error {
  c.mu.Lock()
  c.reduceFinished++
  c.reduceTaskLog[args.ReduceTaskNumber] = 2
  c.mu.Unlock()
  return nil
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
  c.mu.Lock()

  if c.mapFinished < c.nMap {
    allocate := -1
    for i := 0; i < c.nMap; i++ {
      if c.mapTaskLog[i] == 0 {
        allocate = i
        break
      }
    }

    if allocate == -1 {
      reply.TaskType = TaskTypeWait
      c.mu.Unlock()
    } else {
      reply.NReduce = c.nReduce
      reply.TaskType = TaskTypeMap
      reply.MapTaskNumber = allocate
      reply.Filename = c.files[allocate]
      c.mapTaskLog[allocate] = 1
      c.mu.Unlock()
      log.Printf("Allocate map task %v\n", reply.Filename)
      go func() {
        time.Sleep(time.Duration(10) * time.Second)
        c.mu.Lock()
        if c.mapTaskLog[allocate] == 1 {
          c.mapTaskLog[allocate] = 0
        }
        c.mu.Unlock()
      }()
    }

  } else if c.mapFinished == c.nMap && c.reduceFinished < c.nReduce {

    allocate := -1
    for i:= 0; i < c.nReduce; i++ {
      if c.reduceTaskLog[i] == 0 {
        allocate = i
        break
      }
    }

    if allocate == -1 {
      reply.TaskType = TaskTypeWait
      c.mu.Unlock()
    } else {
      reply.TaskType = TaskTypeReduce
      reply.NMap = c.nMap
      reply.NReduce = c.nReduce
      reply.ReduceTaskNumber = allocate
      c.reduceTaskLog[allocate] = 1
      log.Printf("Allocate reduce task %v\n", allocate)
      c.mu.Unlock()
      go func() {
        time.Sleep(time.Duration(10) * time.Second)
        c.mu.Lock()
        if c.reduceTaskLog[allocate] == 1 {
          c.reduceTaskLog[allocate] = 0
        }
        c.mu.Unlock()
      }()
    }

  } else {
    reply.TaskType = TaskTypeNone
    c.mu.Unlock()
  }

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
	ret := false

	// Your code here.

  c.mu.Lock()
  ret = c.nMap == c.reduceFinished

  c.mu.Unlock()
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
  c.files = files
  c.nMap = len(files)
  c.nReduce = len(files)
  c.mapTaskLog = make([]int, c.nMap)
  c.reduceTaskLog = make([]int, c.nReduce)

	c.server()
	return &c
}
