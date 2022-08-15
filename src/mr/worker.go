package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

  for {
    args := WorkerArgs{}
    reply := WorkerReply{}
    ok := call("Coordinator.AllocateTask", &args, &reply)

    if !ok || reply.TaskType == TaskTypeNone {
      log.Println("Worker exiting")
      break
    }

    if reply.TaskType == TaskTypeMap {
      
      intermediate := []KeyValue{}
      file, err := os.Open(reply.Filename)
      if err != nil {
        log.Fatalf("Can not open %v\n", reply.Filename)
      }

      content, err := ioutil.ReadAll(file)
      if err != nil {
        log.Fatalf("Can not read %v\n", reply.Filename)
      }
      file.Close()

      kva := mapf(reply.Filename, string(content))
      intermediate = append(intermediate, kva...)

      buckets := make([][]KeyValue, reply.NReduce)
      for i := range buckets {
        buckets[i] = []KeyValue{}
      }

      for _, kva := range intermediate {
        buckets[ihash(kva.Key) % reply.NReduce] = append(buckets[ihash(kva.Key) % reply.NReduce], kva)
      }

      for i := range buckets {
        oname := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i)
        log.Printf("Writing to file %v\n", oname)
        ofile, _ := ioutil.TempFile("", oname + "*")
        enc := json.NewEncoder(ofile)
        for _, kva := range buckets[i] {
          err := enc.Encode(&kva)
          if err != nil {
            log.Fatalf("Can not write into %v\n", oname)
          }
        }
        os.Rename(ofile.Name(), oname)
        ofile.Close()
      }

      finishedArgs := WorkerArgs{reply.MapTaskNumber, -1}
      finishedReply := WorkerReply{}

      call("Coordinator.MapFinished", &finishedArgs, &finishedReply)

    } else if reply.TaskType == TaskTypeReduce {
      intermediate := []KeyValue{}
      for i:= 0; i < reply.NMap; i++ {
        iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
        log.Printf("Openning file %v\n", iname)
        file, err := os.Open(iname)
        if err != nil {
          log.Fatalf("Reduce: Can not open %v\n", err)
        }
        dec := json.NewDecoder(file)
        for {
          var kv KeyValue
          if err := dec.Decode(&kv); err != nil {
            break
          }
          intermediate = append(intermediate, kv)
        }
        file.Close()
      }
      sort.Sort(ByKey(intermediate))
      oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskNumber)
      ofile, _ := ioutil.TempFile("", oname + "*")

      i := 0
      for i < len(intermediate) {
        j := i + 1
        for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
          j++
        }
        values := []string{}
        for k:= i; k < j; k++ {
          values = append(values, intermediate[k].Value)
        }
        output := reducef(intermediate[i].Key, values)
        fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
        i = j
      }
      os.Rename(ofile.Name(), oname)
      ofile.Close()

      for i := 0; i < reply.NMap; i++ {
        iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
        err := os.Remove(iname)
        if err != nil {
          log.Fatalf("Can not open delete" + iname)
        }
      }

      finishedArgs := WorkerArgs{-1, reply.ReduceTaskNumber}
      finishedReply := WorkerReply{}
      call("Coordinator.ReduceFinished", &finishedArgs, &finishedReply)
      
    }
    time.Sleep(time.Second)
  }

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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
