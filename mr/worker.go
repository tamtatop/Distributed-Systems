package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"



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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		var reply *TaskAssign
		var err bool
		reply, err = CallToRequestTask()
		if !err {
			return
		}
		switch reply.TaskType {
		 case MapTask:
		 	doMapTask(reply, mapf)
		 case ReduceTask:
		 	doReduceTask(reply, reducef)
		 case WaitForTask:
		 	time.Sleep(1000)
		 case NoMoreTasks:
		 	return
		 default:
		 	return
		 }

	}

}

func CallFinishedTask(taskType int, taskInd int){
	fin := TaskFinished{}
	fin.TaskType = taskType
	fin.TaskIndex = taskInd

	var resp interface{}
	call("Master.MarkFinishedTask", &fin, &resp)
	return
}

func doMapTask(reply *TaskAssign, mapf func(string, string) []KeyValue){
	// fmt.Println("will do map")
	defer CallFinishedTask(MapTask, reply.TaskIndex)

	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	intermediate := mapf(reply.FileName, string(content))
	
	sort.Sort(ByKey(intermediate))

	ofiles := make([]*os.File, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskIndex, i)
		ofile, _ := ioutil.TempFile("./", "tmp-file")
		ofiles[i] = ofile

		defer ofile.Close()
		defer os.Rename(ofile.Name(), oname)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		reduceInd := ihash(intermediate[i].Key) % reply.NReduce
		enc := json.NewEncoder(ofiles[reduceInd])
		for k := i; k < j; k++ {
			enc.Encode(&intermediate[k]) 
		}
		i = j
	}
	return
}

func doReduceTask(reply *TaskAssign, reducef func(string, []string) string){
	defer CallFinishedTask(ReduceTask, reply.TaskIndex)

	oname := fmt.Sprintf("mr-out-%d", reply.TaskIndex)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	reduceInput := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		mapFileName := fmt.Sprintf("mr-%d-%d", i, reply.TaskIndex)
		mapFile, err := os.Open(mapFileName)
		if err != nil {
			log.Fatalf("cannot open %v", mapFileName)
		}

		dec := json.NewDecoder(mapFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			reduceInput = append(reduceInput, kv)
		}
	}

	sort.Sort(ByKey(reduceInput))

	i := 0
	for i < len(reduceInput) {
		j := i + 1
		for j < len(reduceInput) && reduceInput[j].Key == reduceInput[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, reduceInput[k].Value)
		}
		output := reducef(reduceInput[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", reduceInput[i].Key, output)

		i = j
	}
}

func CallToRequestTask() (*TaskAssign, bool) {
	var req interface{}
	resp := TaskAssign{}
	err := call("Master.AssignTask", &req, &resp)
	return &resp, err
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
